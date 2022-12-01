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
  name = var.scope_name  
}

resource "databricks_secret" "TransientStorageAccountName1sec" {
  key          = "TransientStorageAccountName"
  string_value = var.transient_name
  scope        = databricks_secret_scope.internal.id
}

resource "databricks_secret" "TransientStorageAccountKey1sec" {
  key          = "TransientStorageAccountKey"
  string_value = var.transient_key
  scope        = databricks_secret_scope.internal.id
  // depends_on   = [azurerm_storage_account.tfadminblob]
}

resource "databricks_secret" "BronzeStorageAccountName1sec" {
  key          = "BronzeStorageAccountName"
  string_value = var.bronze_name
  scope        = databricks_secret_scope.internal.id  
}

resource "databricks_secret" "BronzeStorageAccountKey1sec" {
  key          = "BronzeStorageAccountKey"
  string_value = var.bronze_key
  scope        = databricks_secret_scope.internal.id
  // depends_on   = [azurerm_storage_account.nintexadlsbronze]
}

resource "databricks_secret" "SilverGoldStorageAccountName1sec" {
  key          = "SilverGoldStorageAccountName"
  string_value = var.silver_name
  scope        = databricks_secret_scope.internal.id
}

resource "databricks_secret" "SilverGoldStorageAccountKey1sec" {
  key          = "SilverGoldStorageAccountKey"
  string_value = var.silver_key
  scope        = databricks_secret_scope.internal.id
  // depends_on   = [azurerm_storage_account.nintexadlssilver]
}
