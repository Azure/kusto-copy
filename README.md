#   Kusto Copy

This project aims at building a tool to provide easy copying capabilities between Azure Data Explorer (Kusto) databases.  This enables scenarios such as cluster migration, replication across region (BCDR) & others

## Limitations

The tool has the following known limitations:

*   Can only have one source cluster per running instance (can have open-ended number of destination clusters)
*   Is bound by the capacity of the source cluster to export data and the destination clusters to ingest the data
*   Number of databases:  the tool isn't optimized to work with lots of small databases totalling a huge amount (1000+) of tables
*   The tool doesn't track purges, row deletions or extent deletions
*   Update policies aren't taken into account ; tables are replicated assuming it is all "original content"

## Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.opensource.microsoft.com.

When you submit a pull request, a CLA bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., status check, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

## Trademarks

This project may contain trademarks or logos for projects, products, or services. Authorized use of Microsoft 
trademarks or logos is subject to and must follow 
[Microsoft's Trademark & Brand Guidelines](https://www.microsoft.com/en-us/legal/intellectualproperty/trademarks/usage/general).
Use of Microsoft trademarks or logos in modified versions of this project must not cause confusion or imply Microsoft sponsorship.
Any use of third-party trademarks or logos are subject to those third-party's policies.
