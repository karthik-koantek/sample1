data "azurerm_resource_group" "tfadmin-rg" {
  name = var.resource_group_name
}

###########Azure Databricks ##########################################################
#create azure virtual network
resource "azurerm_virtual_network" "tfadmin-vnet" {
  name                = var.vnet_name
  address_space       = var.vnet_ipaddress
  location            = data.azurerm_resource_group.tfadmin-rg.location
  resource_group_name = data.azurerm_resource_group.tfadmin-rg.name
}

resource "azurerm_subnet" "public" {
  name                 = var.public_subnet_name
  resource_group_name  = data.azurerm_resource_group.tfadmin-rg.name
  virtual_network_name = azurerm_virtual_network.tfadmin-vnet.name
  address_prefixes     = var.public_subnet_ips

  delegation {
    name = "${var.name}-databricks-del"

    service_delegation {
      actions = [
          "Microsoft.Network/virtualNetworks/subnets/join/action",
          "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
          "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
        ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet" "private" {
  name                 = var.private_subnet_name
  resource_group_name  = data.azurerm_resource_group.tfadmin-rg.name
  virtual_network_name = azurerm_virtual_network.tfadmin-vnet.name
  address_prefixes     = var.private_subnet_ips

  delegation {
    name = "${var.name}-databricks-del"

   service_delegation {
      actions = [
          "Microsoft.Network/virtualNetworks/subnets/join/action",
          "Microsoft.Network/virtualNetworks/subnets/prepareNetworkPolicies/action",
          "Microsoft.Network/virtualNetworks/subnets/unprepareNetworkPolicies/action",
        ]
      name = "Microsoft.Databricks/workspaces"
    }
  }
}

resource "azurerm_subnet_network_security_group_association" "private" {
  subnet_id                 = azurerm_subnet.private.id
  network_security_group_id = azurerm_network_security_group.tfadmin-nsg.id
}

resource "azurerm_subnet_network_security_group_association" "public" {
  subnet_id                 = azurerm_subnet.public.id
  network_security_group_id = azurerm_network_security_group.tfadmin-nsg.id
}

resource "azurerm_network_security_group" "tfadmin-nsg" {
  name                = var.databricks_nsg_name
  location            = data.azurerm_resource_group.tfadmin-rg.location
  resource_group_name = data.azurerm_resource_group.tfadmin-rg.name
}

resource "azurerm_databricks_workspace" "tfadmin-dbwx" {
  name                        = var.databricks_workspace_name
  resource_group_name         = data.azurerm_resource_group.tfadmin-rg.name
  location                    = data.azurerm_resource_group.tfadmin-rg.location
  sku                         = "premium"
  managed_resource_group_name = "${var.name}-DBW-managed-without-lb"
  public_network_access_enabled = true

  custom_parameters {
    no_public_ip        = true
    public_subnet_name  = azurerm_subnet.public.name
    private_subnet_name = azurerm_subnet.private.name
    virtual_network_id  = azurerm_virtual_network.tfadmin-vnet.id

   public_subnet_network_security_group_association_id  = azurerm_subnet_network_security_group_association.public.id
   private_subnet_network_security_group_association_id = azurerm_subnet_network_security_group_association.private.id
  }

}

output "id" {
  value = azurerm_databricks_workspace.tfadmin-dbwx.id
}

output "workspace_id" {
  value = azurerm_databricks_workspace.tfadmin-dbwx.workspace_id
}

output "databricks_instance" {
  value = "https://${azurerm_databricks_workspace.tfadmin-dbwx.workspace_url}/"
}

 resource "databricks_workspace_conf" "thisconf" {
  custom_config = {
    "enableWebTerminal" : true,
    "enableDbfsFileBrowser" : true,
  }
  depends_on   = [azurerm_databricks_workspace.tfadmin-dbwx]
}

// provider "databricks" {
//   host                        = azurerm_databricks_workspace.tfadmin-dbwx.workspace_url
//   azure_workspace_resource_id = azurerm_databricks_workspace.tfadmin-dbwx.id
// }

