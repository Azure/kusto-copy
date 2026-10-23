# ⚙️ YAML configuration

[Documentation home](README.md) | [CLI reference](parameters.md) |
[Multi-table recipe](scenarios.md#copy-multiple-tables)

Define multiple table-copy activities in one file, then run `kc -y my-activities.yaml`.
This page is the **schema reference**; use the multi-table recipe above for a runnable example.

> [!TIP]
> CLI options override YAML values. Keep activity names and staging directories stable
> when [resuming a job](advanced.md#resume-and-restart).

<a id="format"></a>

## 📄 Format

The values below describe types, not a runnable configuration. Optional fields can be omitted.

```yaml
activities:
- activityName: "string"
  source:
    clusterUri: "uri"
    databaseName: "string"
    tableName: "string"
    entityGroup: "string"
  destination:
    clusterUri: "uri"
    databaseName: "string"
    tableName: "string"
  kqlQuery: "string"
  postPlanningKqlQuery: "string"
stagingStorageDirectories:
- "uri"
exportCount: "integer"
copyMode: "string"
managedIdentityClientId: "string"
```

<a id="property-values"></a>

## 🗂️ Property values

### Job settings

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| activities | List | Yes | — | One or more [activities](#activities) |
| stagingStorageDirectories | URI list | Yes | — | [Staging folders][storage] |
| copyMode | String | No | BackfillOnly | [Copy mode][mode] |
| exportCount | Integer | No | 20 | [Parallel export limit][export] |
| managedIdentityClientId | String | No | — | [Managed identity client ID][identity] |

### Activities

Each activity connects one source table to one destination table.

| Name | Type | Required | Default | Description |
|------|------|----------|---------|-------------|
| activityName | String | Yes | — | Name shown in progress output |
| source | Object | Yes | — | [Source settings](#source) |
| destination | Object | Yes | — | [Destination settings](#destination) |
| kqlQuery | String | No | — | [Row-level query][query] |
| postPlanningKqlQuery | String | No | — | [Post-planning KQL query][postPlanning] |

#### Source

| Name | Type | Required | Description |
|------|------|----------|-------------|
| clusterUri | URI | Yes | Source cluster URI |
| databaseName | String | Yes | Source database |
| tableName | String | Yes | Source table |
| entityGroup | String | No | Existing [entity group][entity-group] in the source database |

**Using an entity group?** Kusto Copy expands the activity into one activity for each entity
reference in that group.

#### Destination

| Name | Type | Required | Default |
|------|------|----------|---------|
| clusterUri | URI | Yes | — |
| databaseName | String | Yes | — |
| tableName | String | No | Source table name |

> [!IMPORTANT]
> Destination tables must already exist with compatible schemas. YAML defines copy
> activities; it does not create the final tables.

<a id="post-planning-kql-query"></a>

## 🧮 Post-planning KQL query

Use the YAML activity property `postPlanningKqlQuery` to run additional KQL on the rows
selected by the regular query during the export phase only. It is intentionally not used by
planning, so it does not influence block selection or query planning cost.

When `postPlanningKqlQuery` is non-empty, destination extent `CreationTime` is no longer
matched to the source, even if the query only filters or projects rows. Without a
post-planning query, source extent creation times are preserved.

This is useful for aggregations, expensive filters, or other transformations that you want to
apply only to exported data. A common example is summarizing by hour, but a regex-based filter
can also be appropriate if you want to trade planning cost for export-only processing.

For example, summarize temperature readings by hour and widget:

```yaml
activities:
- activityName: "hourly-temperature"
  kqlQuery: "| where isnotnull(Temperature)"
  postPlanningKqlQuery: >-
    | summarize TotalTemp=sum(Temperature)
    by bin(Timestamp, 1h), WidgetId
```

Use `>-` to split long queries across lines: YAML folds equally indented lines into spaces
and removes the trailing newline. More-indented lines retain line breaks, which KQL also
accepts.

The destination table's schema must match the post-planning query output, not the original
source rows. Because the query is not part of planning, the exported result may be smaller or
more selective than the planner's assumptions; choose carefully based on your workload.

See the [aggregation recipe](scenarios.md#aggregate-before-copying) for a complete example.

[storage]: parameters.md#adls-gen-2-containers
[mode]: parameters.md#copy-mode
[export]: parameters.md#parallel-export
[identity]: parameters.md#client-id
[query]: parameters.md#query
[postPlanning]: #post-planning-kql-query
[entity-group]: https://learn.microsoft.com/kusto/management/entity-groups