provider "azurerm" {
    features {
     key_vault {
      purge_soft_delete_on_destroy = true
     }
    }
} 

terraform {
  required_providers {
    azurecaf = {
      source = "aztfmod/azurecaf"
      version = "2.0.0-preview-3"
    }
     databricks = {
      source  = "databrickslabs/databricks"
      version = "0.6.2"
    }
  }
}

provider "azurecaf" {
  # Configuration options
}

provider "databricks" {
  host                        = module.Databricks.databricks_instance
  azure_workspace_resource_id = module.Databricks.id
}