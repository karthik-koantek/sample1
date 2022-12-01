// terraform {
//   backend "azurerm" {
//   }
// }

provider "azurerm" {
    features {
     key_vault {
      purge_soft_delete_on_destroy = true
     }
    }
} 


# Create a resource group
resource "azurerm_resource_group" "tfadminrg" {
  name     = var.rg_name
  location = var.location
}

resource "azurerm_storage_account" "tfadminstate" {
  name                     = var.blob_state
  resource_group_name      = azurerm_resource_group.tfadminrg.name
  location                 = azurerm_resource_group.tfadminrg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
}
resource "azurerm_storage_container" "Blob1" {
  name                  = var.container_name
  storage_account_name  = azurerm_storage_account.tfadminstate.name
  container_access_type = "private"
  depends_on = [azurerm_storage_account.tfadminstate]
}