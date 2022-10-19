Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

$Resources = Get-AzResource -ResourceGroupName $ResourceGroupName
$VirtualNetworkResource = $Resources | Where-Object {$_.ResourceType -eq "Microsoft.Network/virtualNetworks"}
$VirtualNetwork = Get-AzVirtualNetwork -Name $VirtualNetworkResource.Name -ResourceGroupName $ResourceGroupName
$PrivateSubnet = $VirtualNetwork.Subnets | Where-Object {$_.Name -eq "private-subnet"}
$PrivateSubnetDelegation = Get-AzDelegation -Subnet $PrivateSubnet
$PublicSubnet = $VirtualNetwork.Subnets | Where-Object {$_.Name -eq "Public-subnet"}
$PublicSubnetDelegation = Get-AzDelegation -Subnet $PublicSubnet
$NetworkSecurityGroup = Get-AzNetworkSecurityGroup -ResourceGroupName $ResourceGroupName
$DatabricksWorkerToWorkerInbound = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-worker-inbound"}
$DatabricksControlPlaneSSH = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-control-plane-ssh"}
$DatabricksControlPlaneWorkerProxy = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-control-plane-worker-proxy"}
$DatabricksWorkerToWebapp = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-webapp"}
$DatabricksWorkerToSql = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-sql"}
$DatabricksWorkerToStorage = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-storage"}
$DatabricksWorkerToWorkerOutbound = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-worker-outbound"}
$DatabricksWorkerToAny = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "databricks-worker-to-any"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-ssh"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-proxy"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound"}
$MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub = $NetworkSecurityGroup.SecurityRules | Where-Object {$_.Name -eq "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub"}
$AllowVnetInBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "AllowVnetInBound"}
$AllowAzureLoadBalancerInBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "AllowAzureLoadBalancerInBound"}
$DenyAllInBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "DenyAllInBound"}
$AllowVnetOutBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "AllowVnetOutBound"}
$AllowInternetOutBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "AllowInternetOutBound"}
$DenyAllOutBound = $NetworkSecurityGroup.DefaultSecurityRules | Where-Object {$_.Name -eq "DenyAllOutBound"}

Describe 'Virtual Network' {
    Context 'Virtual Network' {
        It 'Virtual Network exists' {
            $VirtualNetwork | Should -Be $true
        }
        It 'CIDR IP Address and range should be 10.179.0.0/16' {
            $VirtualNetwork.AddressSpace.AddressPrefixes | Should -Be "10.179.0.0/16"
        }
    }
    Context 'Private Subnet' {
        It 'Private Subnet exists' {
            $PrivateSubnet | Should -Be $true
        }
        It 'CIDR IP Address and range should be 10.179.0.0/18' {
            $PrivateSubnet.AddressPrefix | Should -Be "10.179.0.0/18"
        }
        It 'Private Endpoint Network Policies should be enabled' {
            $PrivateSubnet.PrivateEndpointNetworkPolicies | Should -Be "Enabled"
        }
        It 'Private Subnet Network Security Group should exist' {
            $PrivateSubnet.NetworkSecurityGroup.Id | Should -Be $true
        }
        It 'Private Subnet Service Endpoint should allow Microsoft.Storage' {
            "Microsoft.Storage" | Should -BeIn $PrivateSubnet.ServiceEndpoints.Service
        }
        It 'Private Subnet Service Endpoint should allow Microsoft.Sql' {
            "Microsoft.Sql" | Should -BeIn $PrivateSubnet.ServiceEndpoints.Service
        }
        It 'Private Subnet delegation for Databricks workspace' {
            $PrivateSubnetDelegation.ServiceName | Should -Be "Microsoft.Databricks/Workspaces"
        }
    }
    Context 'Public Subnet' {
        It 'Public Subnet exists' {
            $PublicSubnet | Should -Be $true
        }
        It 'CIDR IP Address and range should be 10.179.64.0/18' {
            $PublicSubnet.AddressPrefix | Should -Be "10.179.64.0/18"
        }
        It 'Private Endpoint Network Policies should be enabled' {
            $PrivateSubnet.PrivateEndpointNetworkPolicies | Should -Be "Enabled"
        }
        It 'Public Subnet Network Security Group should exist' {
            $PublicSubnet.NetworkSecurityGroup.Id | Should -Be $true
        }
        It 'Public Subnet Service Endpoint should allow Microsoft.Storage' {
            "Microsoft.Storage" | Should -BeIn $PublicSubnet.ServiceEndpoints.Service
        }
        It 'Public Subnet Service Endpoint should allow Microsoft.Sql' {
            "Microsoft.Sql" | Should -BeIn $PublicSubnet.ServiceEndpoints.Service
        }
        It 'Public Subnet delegation for Databricks workspace' {
            $PublicSubnetDelegation.ServiceName | Should -Be "Microsoft.Databricks/Workspaces"
        }
    }
    Context 'Network Security Group' {
        It 'Network Security Group should exist' {
            $NetworkSecurityGroup | Should -Be $true
        }
        It 'Should contain 2 subnets' {
            $NetworkSecurityGroup.Subnets.Count | Should -BeExactly 2
        }
        It 'Should Associate public subnet' {
            $PublicSubnet.Id | Should -BeIn $NetworkSecurityGroup.Subnets.Id
        }
        It 'Should Associate private subnet' {
            $PrivateSubnet.Id | Should -BeIn $NetworkSecurityGroup.Subnets.Id
        }
        Context 'Security Rules' {
            Context 'Rules Exist' {
                It 'Security Rule databricks-worker-to-worker-inbound should exist' {
                    "databricks-worker-to-worker-inbound" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-control-plane-ssh should exist' {
                    "databricks-control-plane-ssh" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-control-plane-worker-proxy should exist' {
                    "databricks-control-plane-worker-proxy" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-worker-to-webapp should exist' {
                    "databricks-worker-to-webapp" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-worker-to-sql should exist' {
                    "databricks-worker-to-sql" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-worker-to-storage should exist' {
                    "databricks-worker-to-storage" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-worker-to-worker-outbound should exist' {
                    "databricks-worker-to-worker-outbound" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule databricks-worker-to-any should exist' {
                    "databricks-worker-to-any" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-inbound" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-ssh should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-ssh" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-proxy should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-control-plane-to-worker-proxy" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-databricks-webapp" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-sql" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-storage" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-worker-outbound" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
                It 'Security Rule Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub should exist' {
                    "Microsoft.Databricks-workspaces_UseOnly_databricks-worker-to-eventhub" | Should -BeIn $NetworkSecurityGroup.SecurityRules.Name
                }
            }
            Context 'Databricks Worker to Worker Inbound' {
                It 'Is Inbound' {
                    $DatabricksWorkerToWorkerInbound.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 200' {
                    $DatabricksWorkerToWorkerInbound.Priority | Should -BeExactly 200
                }
                It 'Allows' {
                    $DatabricksWorkerToWorkerInbound.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToWorkerInbound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToWorkerInbound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToWorkerInbound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is Virtual Network' {
                    $DatabricksWorkerToWorkerInbound.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is *' {
                    $DatabricksWorkerToWorkerInbound.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Databricks Control Plane' {
                It 'Is Inbound' {
                    $DatabricksControlPlaneSSH.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 103' {
                    $DatabricksControlPlaneSSH.Priority | Should -BeExactly 103
                }
                It 'Allows' {
                    $DatabricksControlPlaneSSH.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksControlPlaneSSH.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksControlPlaneSSH.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 22' {
                    $DatabricksControlPlaneSSH.DestinationPortRange | Should -BeExactly 22
                }
                It 'Source Address Prefix is 40.83.178.242/32' {
                    $DatabricksControlPlaneSSH.SourceAddressPrefix | Should -Be "40.83.178.242/32"
                }
                It 'Destination Address Prefix is *' {
                    $DatabricksControlPlaneSSH.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Databricks Control Plane Worker Proxy' {
                It 'Is Inbound' {
                    $DatabricksControlPlaneWorkerProxy.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 110' {
                    $DatabricksControlPlaneWorkerProxy.Priority | Should -BeExactly 110
                }
                It 'Allows' {
                    $DatabricksControlPlaneWorkerProxy.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksControlPlaneWorkerProxy.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksControlPlaneWorkerProxy.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 5557' {
                    $DatabricksControlPlaneWorkerProxy.DestinationPortRange | Should -BeExactly 5557
                }
                It 'Source Address Prefix is 40.83.178.242/32' {
                    $DatabricksControlPlaneWorkerProxy.SourceAddressPrefix | Should -Be "40.83.178.242/32"
                }
                It 'Destination Address Prefix is *' {
                    $DatabricksControlPlaneWorkerProxy.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Databricks Worker to Webapp' {
                It 'Is Outbound' {
                    $DatabricksWorkerToWebapp.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 105' {
                    $DatabricksWorkerToWebapp.Priority | Should -BeExactly 105
                }
                It 'Allows' {
                    $DatabricksWorkerToWebapp.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToWebapp.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToWebapp.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToWebapp.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DatabricksWorkerToWebapp.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is 40.118.174.12/32' {
                    $DatabricksWorkerToWebapp.DestinationAddressPrefix | Should -Be "40.118.174.12/32"
                }
            }
            Context 'Databricks Worker to SQL' {
                It 'Is Outbound' {
                    $DatabricksWorkerToSql.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 110' {
                    $DatabricksWorkerToSql.Priority | Should -BeExactly 110
                }
                It 'Allows' {
                    $DatabricksWorkerToSql.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToSql.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToSql.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToSql.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DatabricksWorkerToSql.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is Sql' {
                    $DatabricksWorkerToSql.DestinationAddressPrefix | Should -Be "Sql"
                }
            }
            Context 'Databricks Worker to Storage' {
                It 'Is Outbound' {
                    $DatabricksWorkerToStorage.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 120' {
                    $DatabricksWorkerToStorage.Priority | Should -BeExactly 120
                }
                It 'Allows' {
                    $DatabricksWorkerToStorage.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToStorage.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToStorage.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToStorage.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DatabricksWorkerToStorage.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is Storage' {
                    $DatabricksWorkerToStorage.DestinationAddressPrefix | Should -Be "Storage"
                }
            }
            Context 'Databricks Worker to Worker Outbound' {
                It 'Is Outbound' {
                    $DatabricksWorkerToWorkerOutbound.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 130' {
                    $DatabricksWorkerToWorkerOutbound.Priority | Should -BeExactly 130
                }
                It 'Allows' {
                    $DatabricksWorkerToWorkerOutbound.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToWorkerOutbound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToWorkerOutbound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToWorkerOutbound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DatabricksWorkerToWorkerOutbound.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $DatabricksWorkerToWorkerOutbound.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Databricks Worker to Any' {
                It 'Is Outbound' {
                    $DatabricksWorkerToAny.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 140' {
                    $DatabricksWorkerToAny.Priority | Should -BeExactly 140
                }
                It 'Allows' {
                    $DatabricksWorkerToAny.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $DatabricksWorkerToAny.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DatabricksWorkerToAny.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DatabricksWorkerToAny.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DatabricksWorkerToAny.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is *' {
                    $DatabricksWorkerToAny.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-worker-inbound' {
                It 'Is Inbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 100' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.Priority | Should -BeExactly 100
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.Access | Should -Be "Allow"
                }
                It 'Protocol is Any' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerInbound.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-control-plane-to-worker-ssh' {
                It 'Is Inbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 101' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.Priority | Should -BeExactly 101
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 22' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.DestinationPortRange | Should -BeExactly 22
                }
                It 'Source Address Prefix is AzureDatabricks' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.SourceAddressPrefix | Should -Be "AzureDatabricks"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerSSH.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-control-plane-to-worker-proxy' {
                It 'Is Inbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 102' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.Priority | Should -BeExactly 102
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 5557' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.DestinationPortRange | Should -BeExactly 5557
                }
                It 'Source Address Prefix is AzureDatabricks' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.SourceAddressPrefix | Should -Be "AzureDatabricks"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksControlPlaneToWorkerProxy.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-databricks-webapp' {
                It 'Is Outbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 100' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.Priority | Should -BeExactly 100
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 443' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.DestinationPortRange | Should -BeExactly 443
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is AzureDatabricks' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToDatabricksWebapp.DestinationAddressPrefix | Should -Be "AzureDatabricks"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-sql' {
                It 'Is Outbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 101' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.Priority | Should -BeExactly 101
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 3306' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.DestinationPortRange | Should -BeExactly 3306
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is Sql' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToSql.DestinationAddressPrefix | Should -Be "Sql"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-storage' {
                It 'Is Outbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 102' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.Priority | Should -BeExactly 102
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 443' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.DestinationPortRange | Should -BeExactly 443
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is Storage' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToStorage.DestinationAddressPrefix | Should -Be "Storage"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-storage' {
                It 'Is Outbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 103' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.Priority | Should -BeExactly 103
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.Access | Should -Be "Allow"
                }
                It 'Protocol is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToWorkerOutbound.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Databricks Workspaces Use Only databricks-worker-to-eventhub' {
                It 'Is Outbound' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 104' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.Priority | Should -BeExactly 104
                }
                It 'Allows' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.Access | Should -Be "Allow"
                }
                It 'Protocol is tcp' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.Protocol | Should -Be "tcp"
                }
                It 'Source Port Range is *' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is 9093' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.DestinationPortRange | Should -BeExactly 9093
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is EventHub' {
                    $MicrosoftDatabricksWorkspacesUseOnlyDatabricksWorkerToEventhub.DestinationAddressPrefix | Should -Be "EventHub"
                }
            }
        }
        Context 'Default Security Rules' {
            Context 'Rules Exist' {
                It 'Default Security Rule AllowVnetInBound should exist' {
                    "AllowVnetInBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
                It 'Default Security Rule AllowAzureLoadBalancerInBound should exist' {
                    "AllowAzureLoadBalancerInBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
                It 'Default Security Rule DenyAllInBound should exist' {
                    "DenyAllInBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
                It 'Default Security Rule AllowVnetOutBound should exist' {
                    "AllowVnetOutBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
                It 'Default Security Rule AllowInternetOutBound should exist' {
                    "AllowInternetOutBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
                It 'Default Security Rule DenyAllOutBound should exist' {
                    "DenyAllOutBound" | Should -BeIn $NetworkSecurityGroup.DefaultSecurityRules.Name
                }
            }
            Context 'Allow Vnet Inbound' {
                It 'Is Inbound' {
                    $AllowVnetInBound.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 65000' {
                    $AllowVnetInBound.Priority | Should -BeExactly 65000
                }
                It 'Allows' {
                    $AllowVnetInBound.Access | Should -Be "Allow"
                }
                It 'Protocol is *' {
                    $AllowVnetInBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $AllowVnetInBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $AllowVnetInBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $AllowVnetInBound.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $AllowVnetInBound.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Allow Azure Load Balancer Inbound' {
                It 'Is Inbound' {
                    $AllowAzureLoadBalancerInBound.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 65001' {
                    $AllowAzureLoadBalancerInBound.Priority | Should -BeExactly 65001
                }
                It 'Allows' {
                    $AllowAzureLoadBalancerInBound.Access | Should -Be "Allow"
                }
                It 'Protocol is *' {
                    $AllowAzureLoadBalancerInBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $AllowAzureLoadBalancerInBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $AllowAzureLoadBalancerInBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is AzureLoadBalancer' {
                    $AllowAzureLoadBalancerInBound.SourceAddressPrefix | Should -Be "AzureLoadBalancer"
                }
                It 'Destination Address Prefix is *' {
                    $AllowAzureLoadBalancerInBound.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Deny All Inbound' {
                It 'Is Inbound' {
                    $DenyAllInBound.Direction | Should -Be "Inbound"
                }
                It 'Has Priority 65500' {
                    $DenyAllInBound.Priority | Should -BeExactly 65500
                }
                It 'Denies' {
                    $DenyAllInBound.Access | Should -Be "Deny"
                }
                It 'Protocol is *' {
                    $DenyAllInBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DenyAllInBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DenyAllInBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DenyAllInBound.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is *' {
                    $DenyAllInBound.DestinationAddressPrefix | Should -Be "*"
                }
            }
            Context 'Allow Vnet Outbound' {
                It 'Is Outbound' {
                    $AllowVnetOutBound.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 65000' {
                    $AllowVnetOutBound.Priority | Should -BeExactly 65000
                }
                It 'Allows' {
                    $AllowVnetOutBound.Access | Should -Be "Allow"
                }
                It 'Protocol is *' {
                    $AllowVnetOutBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $AllowVnetOutBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $AllowVnetOutBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is VirtualNetwork' {
                    $AllowVnetOutBound.SourceAddressPrefix | Should -Be "VirtualNetwork"
                }
                It 'Destination Address Prefix is VirtualNetwork' {
                    $AllowVnetOutBound.DestinationAddressPrefix | Should -Be "VirtualNetwork"
                }
            }
            Context 'Allow Internet Outbound' {
                It 'Is Outbound' {
                    $AllowInternetOutBound.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 65001' {
                    $AllowInternetOutBound.Priority | Should -BeExactly 65001
                }
                It 'Allows' {
                    $AllowInternetOutBound.Access | Should -Be "Allow"
                }
                It 'Protocol is *' {
                    $AllowInternetOutBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $AllowInternetOutBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $AllowInternetOutBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $AllowInternetOutBound.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is Internet' {
                    $AllowInternetOutBound.DestinationAddressPrefix | Should -Be "Internet"
                }
            }
            Context 'Deny All Outbound' {
                It 'Is Outbound' {
                    $DenyAllOutBound.Direction | Should -Be "Outbound"
                }
                It 'Has Priority 65500' {
                    $DenyAllOutBound.Priority | Should -BeExactly 65500
                }
                It 'Denies' {
                    $DenyAllOutBound.Access | Should -Be "Deny"
                }
                It 'Protocol is *' {
                    $DenyAllOutBound.Protocol | Should -Be "*"
                }
                It 'Source Port Range is *' {
                    $DenyAllOutBound.SourcePortRange | Should -Be "*"
                }
                It 'Destination Port Range is *' {
                    $DenyAllOutBound.DestinationPortRange | Should -Be "*"
                }
                It 'Source Address Prefix is *' {
                    $DenyAllOutBound.SourceAddressPrefix | Should -Be "*"
                }
                It 'Destination Address Prefix is *' {
                    $DenyAllOutBound.DestinationAddressPrefix | Should -Be "*"
                }
            }
        }
    }
}