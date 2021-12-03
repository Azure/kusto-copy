/**************************************************/
//  Deploy storage account

var uniqueId = uniqueString(resourceGroup().id, 'delta-kusto')

resource storage 'Microsoft.Storage/storageAccounts@2021-06-01' = {
  name: 'lake${uniqueId}'
  location: resourceGroup().location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    isHnsEnabled: true
  }

  resource blobService 'blobServices@2021-06-01' = {
    name: 'default'
    properties: {}

    resource laptopContainer 'containers@2021-06-01' = {
      name: 'laptop'
      properties: {}
    }

    resource testsContainer 'containers@2021-06-01' = {
      name: 'tests'
      properties: {}
    }
  }
}
