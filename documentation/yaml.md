#   YAML file's schema

A YAML file can be used instead of CLI parameters.

This page describes the schema of that file.

##   Format

```YAML
activities:
-   activityName:  "string"
    source:
        clusterUri: "uri"
        databaseName: "string"
        tableName: "string"
        entityGroup: "string"
    destination:
        clusterUri: "uri"
        databaseName: "string"
        tableName:  "string"
    kqlQuery: "string"
stagingStorageDirectories:
- "uri"
exportCount: "integer"
copyMode:  "string"
managedIdentityClientId: "string"
```

##  Property values

Name|Type|Required|Default|Description
-|-|-|-|-
activities|Node|Yes|N/A|One or many activities can be defined.  See [activities section](#activities)
stagingStorageDirectories|`uris` list|Yes|N/A|At least one folder must be specified.  See [ADLS gen 2 containers](parameters.md#adls-gen-2-containers) for details.
copyMode|`string`|No|BackfillOnly|See [Copy Mode](parameters.md#copy-mode)
exportCount|`integer`|No|20|See [Parallel Export](parameters.md#parallel-export)
managedIdentityClientId|`string`|No|N/A|See [Client ID](parameters.md#client-id)

### Activities

Name|Type|Required|Default|Description
-|-|-|-|-
activityName|`string`|Yes|N/A|Name of the activity:  displayed in progress status
source|Node|Yes|N/A|Source table settings.  See [Source](#source)
destination|Node|Yes|N/A|Destination table settings.  See [Destination](#destination)
kqlQuery|`string`|No|N/A|See [query](parameters.md#query)

#### Source

Name|Type|Required|Default|Description
-|-|-|-|-
clusterUri|`uri`|Yes|N/A|URI of the source Kusto cluster
databaseName|`string`|Yes|N/A|Name of the source database
tableName|`string`|Yes|N/A|Name of the source table
entityGroup|`string`|No|N/A|Name of an existing [entity group](https://learn.microsoft.com/kusto/management/entity-groups) in the source database

When `entityGroup` is specified, Kusto Copy expands the configured activity into one activity for each entity reference in the group.

#### Destination

Name|Type|Required|Default|Description
-|-|-|-|-
clusterUri|`uri`|Yes|N/A|URI of the destination Kusto cluster
databaseName|`string`|Yes|N/A|Name of the destination database
tableName|`string`|No|Source table name|Name of the destination table
