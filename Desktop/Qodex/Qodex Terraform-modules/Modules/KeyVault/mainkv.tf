data "azurerm_client_config" "current1" {}

data "azurerm_resource_group" "tfadmin-rg" {
  name = var.resource_group_name
}


###azure Keyvault
resource "azurerm_key_vault" "tfadmin-kv" {
  name                        = var.kv_name
  location                    = data.azurerm_resource_group.tfadmin-rg.location
  resource_group_name         = data.azurerm_resource_group.tfadmin-rg.name
  enabled_for_disk_encryption = true
  tenant_id                   = data.azurerm_client_config.current1.tenant_id
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false

  sku_name = "standard"

  access_policy {
    tenant_id = data.azurerm_client_config.current1.tenant_id
    object_id = data.azurerm_client_config.current1.object_id

    key_permissions = [
      "Get", 
	    "List",
      "Create" 
    ]

    secret_permissions = [
      "List",
      "Set",
      "Get",
      "Delete",
      "Purge",
      "Recover",
      "Backup"
    ]
  }
}
resource "azurerm_key_vault_secret" "TransientStorageAccountName" {
  name         = "TransientStorageAccountName"
  value        = var.transient_name
  key_vault_id = azurerm_key_vault.tfadmin-kv.id
  // depends_on   = [azurerm_key_vault_access_policy.policy]
}


resource "azurerm_key_vault_secret" "TransientStorageAccountKey" {
  name         = "TransientStorageAccountKey"
  value        = var.transient_key
  key_vault_id = azurerm_key_vault.tfadmin-kv.id
 //  depends_on   = [azurerm_storage_account.tfadminblob, azurerm_key_vault.tfadmin-kv]
}
