 output "storage_primary_access_key2" {
   value = azurerm_storage_account.adlsbronze.primary_access_key
   sensitive = true
 }

 output "storage_account_name2" {
   value = azurerm_storage_account.adlsbronze.name
   sensitive = true
 }


 output "storage_account_id2" {
   value = azurerm_storage_account.adlsbronze.id
   sensitive = true
 }

 output "ext_container" {
   value = azurerm_storage_container.ext_storage.name
   sensitive = true
 }