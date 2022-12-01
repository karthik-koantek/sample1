data "azurerm_resource_group" "tfadmin-rg" {
  name = var.resource_group_name
}


#create storage account
resource "azurerm_storage_account" "tfadminblob" {
  name                     = var.blob_name
  resource_group_name      = data.azurerm_resource_group.tfadmin-rg.name
  location                 = data.azurerm_resource_group.tfadmin-rg.location
  account_tier             = "Standard"
  account_replication_type = var.account_replication_type
}

resource "azurerm_storage_container" "Blob" {
  // for_each              = var.storage_blob
  name                  =  var.storage_blob
  storage_account_name  = azurerm_storage_account.tfadminblob.name
  container_access_type = "private"
  depends_on = [azurerm_storage_account.tfadminblob]
}