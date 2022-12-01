data "azurerm_resource_group" "tfadmin-rg" {
  name = var.resource_group_name
}



#####Bronze Zone Storage Account (ADLS Gen2 w/ hierarchical namespaces)
resource "azurerm_storage_account" "adlsbronze" {
  name                     = var.adls_name
  resource_group_name      = data.azurerm_resource_group.tfadmin-rg.name
  location                 = data.azurerm_resource_group.tfadmin-rg.location
  account_tier             = var.account_tier
  account_replication_type = var.account_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = "true"
}

resource "azurerm_storage_data_lake_gen2_filesystem" "adlsbronze" {
  name               = "data"
  storage_account_id = azurerm_storage_account.adlsbronze.id
}

resource "azurerm_storage_container" "ext_storage" {
  name                  = "qsadata"
  storage_account_name  = azurerm_storage_account.adlsbronze.name
  container_access_type = "private"
}