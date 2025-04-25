# Setup guide

This article will guide you through the setup to use Kusto Copy to copy a table from one cluster to another.

## Prerequisites

*   A source Kusto cluster (either Azure Data Explorer or Fabric Eventhouse)
*   A destination Kusto cluster (either Azure Data Explorer or Fabric Eventhouse)
*   An ADLS gen 2 storage account
*   Somewhere to run the Kusto Copy CLI (e.g. a laptop, Cloud Shell, etc.)

## Download Kusto Copy CLI

You can download the CLI by going [here](https://github.com/Azure/kusto-copy/releases).  This is the release page and should show you a list of different releases starting with the latest one:

![Release](artefacts/setup/release.png)

Download the package matching the OS you want to run on (Linux / Mac OS / Windows), uncompress it and you should be good to go.

## Permissions

Kusto Copy runs with an Azure Entra ID identity which identity will:

* Execute queries on the source database:  it needs the *viewer* role on the source database
* Ingest data, create temporary tables and run queries on the destination database:  it needs *admin* role on the destination database
* Export data to the ADLS gen 2 storage account and generate SAS tokens on those blobs:  it needs *XYZ* role on the storage container

 By default, the identity of the user logged into Azure CLI.  In general, that is **your identity**.

### Source Database permissions

There are two ways to assign the viewer role to a principal

### Destination Database permissions

### ADLS Gen2 container permissions

## Running the tool

## Validating the copy