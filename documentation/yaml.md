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
    destination:
        clusterUri: "uri"
        databaseName: "string"
        tableName:  "string"
    kqlQuery: "string"
stagingStorageDirectories:
- "uri"
exportCount: "integer"
managedIdentityClientId: "string"
```

##  Property values

Name|Type|Required|Default|Description
-|-|-|-|-
activities|Node|Yes|N/A|See [activities section](#activities)
stagingStorageDirectories|`uris` list|Yes|N/A|At least one folder must be specified.  See [ADLS gen 2 containers](parameters.md#adls-gen-2-containers) for details.
exportCount|`Integer`|No|20|See [Parallel Export](parameters.md#parallel-export)
managedIdentityClientId|`string`|No|N/A|See [Client ID](parameters.md#client-id)

### Activities

Name|Type|Required|Default|Description
-|-|-|-|-
activityName|`string`|Yes|N/A|Name of the activity:  displayed in progress status
source|Table URI|Yes|N/A|Specify the source table.  See [Database / Table URI](parameters.md#database--table-uri) 
destination|Database or Table URI|Yes|N/A|Specify the destination table (if table is ommited, source table is inferred).  See [Database / Table URI](parameters.md#database--table-uri)
kqlQuery|`string`|No|N/A|See [query](parameters.md#query)
