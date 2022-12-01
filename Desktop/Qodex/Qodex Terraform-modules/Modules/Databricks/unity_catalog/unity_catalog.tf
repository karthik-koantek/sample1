variable "databricks_resource_id" {
  description = "The Azure resource ID for the databricks workspace deployment."
}

data "azurerm_client_config" "current" {
}


resource "azurerm_databricks_access_connector" "unity" {
  name                = var.access_connector_name
  resource_group_name = data.azurerm_resource_group.tfadmin-rg.name
  location            = data.azurerm_resource_group.tfadmin-rg.location
  identity {
    type = "SystemAssigned"
  }
}

resource "azurerm_storage_account" "unity_catalog" {
  name                     = var.uc_storage_name
  resource_group_name      = data.azurerm_resource_group.tfadmin-rg.name
  location                 = data.azurerm_resource_group.tfadmin-rg.location
  tags                     = data.azurerm_resource_group.tfadmin-rg.tags
  account_tier             = "Standard"
  account_replication_type = "GRS"
  is_hns_enabled           = true
}

resource "azurerm_storage_container" "unity_catalog" {
  name                  = var.uc_container_name
  storage_account_name  = azurerm_storage_account.unity_catalog.name
  container_access_type = "private"
}

resource "azurerm_role_assignment" "example" {
  scope                = azurerm_storage_account.unity_catalog.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.unity.identity[0].principal_id
}

 output "storage_key_meta" {
   value = azurerm_storage_account.unity_catalog.primary_access_key
   sensitive = true
 }

 output "storage_account_name_meta" {
   value = azurerm_storage_account.unity_catalog.name
   sensitive = true
 }


##############################link metastore to workspace
resource "databricks_metastore" "this" {
  name = "primary"
  storage_root = format("abfss://%s@%s.dfs.core.windows.net/",
    azurerm_storage_container.unity_catalog.name,
  azurerm_storage_account.unity_catalog.name)
  force_destroy = true
}

resource "databricks_metastore_data_access" "first" {
  metastore_id = databricks_metastore.this.id
  name         = var.data_access_key_name
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.unity.id
  }

  is_default = true
}

resource "databricks_metastore_assignment" "this" {
  workspace_id         = azurerm_databricks_workspace.tfadmin-dbwx.workspace_id
  metastore_id         = databricks_metastore.this.id
  default_catalog_name = "hive_metastore"
}

##########################catalog
resource "databricks_catalog" "sandbox" {
  metastore_id = databricks_metastore.this.id
  name         = var.uc_catalog_name
  comment      = "this catalog is managed by terraform"
  depends_on = [databricks_metastore_assignment.this]
}

// resource "databricks_grants" "sandbox" {
//   catalog = databricks_catalog.sandbox.name
//   grant {
//     principal  = "Data Engineers"
//     privileges = ["USAGE", "CREATE"]
//   }
//   grant {
//     principal  = "Reader"
//     privileges = ["USAGE"]
//   }
// }

resource "databricks_schema" "things" {
  catalog_name = databricks_catalog.sandbox.id
  name         = var.schema_name
  comment      = "this database is managed by terraform"
}

// resource "databricks_grants" "things" {
//   schema = databricks_schema.things.id
//   grant {
//     principal  = "Data Engineers"
//     privileges = ["USAGE"]
//   }
// }

##############################################external storage
// data "azurerm_storage_account" "tfadmin-adls" {
//   name = var.adls_gen2
// }

resource "azurerm_databricks_access_connector" "ext_access_connector" {
  name                = var.ext_access_connector
  resource_group_name = data.azurerm_resource_group.tfadmin-rg.name
  location            = data.azurerm_resource_group.tfadmin-rg.location
  identity {
    type = "SystemAssigned"
  }
}


resource "azurerm_role_assignment" "ext_storage" {
  scope                = var.azure_adls_gen2_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_access_connector.identity[0].principal_id
   depends_on = [var.adls_gen2]
}



resource "databricks_storage_credential" "external" {
  name = azurerm_databricks_access_connector.ext_access_connector.name
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ext_access_connector.id
  }
  comment = "Managed by TF"
  depends_on = [
    databricks_metastore_assignment.this
  ]
}

// resource "databricks_grants" "external_creds" {
//   storage_credential = databricks_storage_credential.external.id
//   grant {
//     principal  = "Data Engineers"
//     privileges = ["CREATE_TABLE"]
//   }
// }

resource "databricks_external_location" "some" {
  name = var.external_location
  url = format("abfss://%s@%s.dfs.core.windows.net/", "${var.adls_container_name}", "${var.adls_gen2_name}")

  credential_name = databricks_storage_credential.external.id
  comment         = "Managed by TF"
  depends_on = [
    databricks_metastore_assignment.this
  ]
}

// resource "databricks_grants" "some" {
//   external_location = databricks_external_location.some.id
//   grant {
//     principal  = "Data Engineers"
//     privileges = ["CREATE_TABLE", "READ_FILES"]
//   }
// }

#################################################################################
resource "databricks_cluster" "unity_sql" {
  cluster_name            = var.unity_cluster_name
  spark_version           = var.spark_version-single
  node_type_id            = var.node_type_id_single
  autotermination_minutes = 20
  enable_elastic_disk     = false
  num_workers             = 2
  azure_attributes {
    availability = "SPOT"
  }
  data_security_mode = "USER_ISOLATION"
  # need to wait until the metastore is assigned
  depends_on = [
    databricks_metastore_assignment.this
  ]
}

resource "databricks_permissions" "cluster_usage" {
  cluster_id = databricks_cluster.unity_sql.cluster_id

   access_control {
    group_name       = databricks_group.spectators.display_name
    permission_level = "CAN_RESTART"
  }
}