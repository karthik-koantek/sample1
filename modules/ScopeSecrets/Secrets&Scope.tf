#########################sectret scope##################
resource "databricks_secret_scope" "this1" {
  name = "demo"
}

###########token
resource "databricks_token" "pat" {
  comment          = "Created from ${abspath(path.module)}"
  // lifetime_seconds = 3600
}

resource "databricks_secret" "token" {
  string_value = databricks_token.pat.token_value
  scope        = databricks_secret_scope.this1.name
  key          = "token"
}


resource "databricks_secret_scope" "internal" {
  name = "internal"  
}


resource "databricks_secret_scope" "metadatadb" {
  name = "metadatadb"
}



# resource "databricks_secret" "TransientStorageAccountName1sec" {
#   key          = "TransientStorageAccountName"
#   string_value = "ascenddevtransient"
#   scope        = databricks_secret_scope.internal.id
# }

# resource "databricks_secret" "TransientStorageAccountKey1sec" {
#   key          = "TransientStorageAccountKey"
#   string_value = data.azurerm_storage_account.example1.primary_access_key
#   scope        = databricks_secret_scope.internal.id
#   depends_on   = [azurerm_storage_account.tfadminblob]
# }


# resource "databricks_secret" "BronzeStorageAccountName1sec" {
#   key          = "BronzeStorageAccountName"
#   string_value = "ascenddevbronze"
#   scope        = databricks_secret_scope.internal.id  
# }

# resource "databricks_secret" "BronzeStorageAccountKey1sec" {
#   key          = "BronzeStorageAccountKey"
#   string_value = data.azurerm_storage_account.example2.primary_access_key
#   scope        = databricks_secret_scope.internal.id
#   depends_on   = [azurerm_storage_account.nintexadlsbronze]
# }

# resource "databricks_secret" "SilverGoldStorageAccountName1sec" {
#   key          = "SilverGoldStorageAccountName"
#   string_value = "ascenddevsilver"
#   scope        = databricks_secret_scope.internal.id
# }

# resource "databricks_secret" "SilverGoldStorageAccountKey1sec" {
#   key          = "SilverGoldStorageAccountKey"
#   string_value = data.azurerm_storage_account.example3.primary_access_key
#   scope        = databricks_secret_scope.internal.id
#   depends_on   = [azurerm_storage_account.nintexadlssilver]
# }

# resource "databricks_secret" "SandboxStorageAccountName1sec" {
#   key          = "SandboxStorageAccountName"
#   string_value = "ascenddevsandbox"
#   scope        = databricks_secret_scope.internal.id
# }

# resource "databricks_secret" "SandboxStorageAccountKey1sec" {
#   key          = "SandboxStorageAccountKey"
#   string_value = data.azurerm_storage_account.example4.primary_access_key
#   scope        = databricks_secret_scope.internal.id
#   depends_on   = [azurerm_storage_account.nintexadlssandbox]
# }


resource "databricks_secret" "bearertoken" {
  string_value = databricks_token.pat.token_value
  scope        = databricks_secret_scope.internal.id
  key          = "BearerToken"
}


resource "databricks_secret" "strgkeyout" {
  string_value = "Null"
  scope        = databricks_secret_scope.internal.name
  key          = "StorageAccountkey"
}

resource "databricks_secret" "strgnameout" {
  string_value = "Null"
  scope        = databricks_secret_scope.internal.name
  key          = "StorageAccountName"
}
