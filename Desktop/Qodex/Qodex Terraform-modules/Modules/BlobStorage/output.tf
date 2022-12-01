 data "azurerm_storage_account" "example1" {
  name                = azurerm_storage_account.tfadminblob.name
  resource_group_name = data.azurerm_resource_group.tfadmin-rg.name
}

output "storage_account_primary_access_key1" {
   value = data.azurerm_storage_account.example1.primary_access_key
   sensitive = true
 }

 output "storage_account_name" {
   value = data.azurerm_storage_account.example1.name
   sensitive = true
 }