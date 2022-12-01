// provider "azurerm" {
//     features {
//      key_vault {
//       purge_soft_delete_on_destroy = true
//      }
//     }
// } 

terraform {
  required_providers {
    azurecaf = {
      source = "aztfmod/azurecaf"
      version = "2.0.0-preview-3"
    }
  }
}

// provider "azurecaf" {
//   # Configuration options
// }
