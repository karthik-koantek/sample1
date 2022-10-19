Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\ADLS\ADLSGen2Module.ps1

$StorageAccounts = Get-StorageAccounts -ResourceGroupName $ResourceGroupName
$TransientStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*blobt"}
$BronzeStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlsb"}
$SilverGoldStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssg"}
$SandboxStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssb"}

#$TransientStorageContainers = $TransientStorageAccount | Get-AzStorageContainer
#$FileSystems = Get-FileSystems -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
#$FileSystemACLPermissions = Get-ACLPermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName -FileSystemName $FileSystems.Name -Path "/"
#$DefaultScopePermissions = $FileSystemACLPermissions | Where-Object {$_.DefaultScope -eq $true -and $_.AccessControlType -eq "User"}
#$ReaderGroup = $DefaultScopePermissions.Permissions | Where-Object {$_.Write -eq $false -and $_.Execute -eq $false}
#$BronzeStorageContainers = (Get-AzStorageContainer -Context $BronzeStorageAccount.Context).Name

Describe 'Storage Unit Tests' {
    Context 'Transient Zone Storage Account' {
        It 'Storage Account should be Gen 2' {
            $TransientStorageAccount.Kind | Should -Be "BlobStorage"
        }
        It 'Storage Account should use Hot Access Tier' {
            $TransientStorageAccount.AccessTier | Should -Be "Hot"
        }
        It 'Storage Account should only support Https Traffic' {
            $TransientStorageAccount.EnableHttpsTrafficOnly | Should -Be "true"
        }
        It 'SkuName should be Standard_LRS' {
            $TransientStorageAccount.Sku.Name | Should -Be "Standard_LRS"
        }
        It 'Tier should be Standard' {
            $TransientStorageAccount.Sku.Tier | Should -Be "Standard"
        }
        It 'NetworkRuleSet should Bypass AzureServices' {
            $TransientStorageAccount.NetworkRuleSet.Bypass | Should -Be "AzureServices"
        }
        It 'NetworkRuleSet Default Action should be deny' {
            $TransientStorageAccount.NetworkRuleSet.DefaultAction | Should -Be "Deny"
        }
        It 'NetworkRuleSet should contain private and public subnets.' {
            $TransientStorageAccount.NetworkRuleSet.VirtualNetworkRules.VirtualNetworkResourceId | Should -HaveCount 2
        }
        #It 'should have a container named polybase' {
        #    "polybase" | Should -BeIn $TransientStorageContainers
        #}
        #It 'should have a container named staging' {
        #    "staging" | Should -BeIn $TransientStorageContainers
        #}
        #It 'should have a container named azuredatafactory' {
        #    "azuredatafactory" | Should -BeIn $TransientStorageContainers
        #}
    }

    Context 'Bronze Zone Storage Account' {
        It 'Storage Account should be Gen 2' {
            $BronzeStorageAccount.Kind | Should -Be "StorageV2"
        }
        It 'Storage Account should use Hot Access Tier' {
            $BronzeStorageAccount.AccessTier | Should -Be "Hot"
        }
        It 'Storage Account should only support Https Traffic' {
            $BronzeStorageAccount.EnableHttpsTrafficOnly | Should -Be "true"
        }
        It 'SkuName should be Standard_LRS' {
            $BronzeStorageAccount.Sku.Name | Should -Be "Standard_LRS"
        }
        It 'Tier should be Standard' {
            $BronzeStorageAccount.Sku.Tier | Should -Be "Standard"
        }
        It 'NetworkRuleSet should Bypass AzureServices' {
            $BronzeStorageAccount.NetworkRuleSet.Bypass | Should -Be "AzureServices"
        }
        It 'NetworkRuleSet Default Action should be deny' {
            $BronzeStorageAccount.NetworkRuleSet.DefaultAction | Should -Be "Deny"
        }
        It 'NetworkRuleSet should contain private and public subnets.' {
            $BronzeStorageAccount.NetworkRuleSet.VirtualNetworkRules.VirtualNetworkResourceId | Should -HaveCount 2
        }
        #It 'Storage Account has a file system named raw' {
        #    $FileSystems[0].Name | Should -Be "raw"
        #}
        #It 'framework File System does not allow public access' {
        #    $FileSystems[0].PublicAccess | Should -Be "Off"
        #}
        #It 'Default Scope Permissions should be set for superuser and 3 AD Groups' {
        #    $DefaultScopePermissions.Count | Should -BeGreaterOrEqual 4
        #}
        #It 'Reader Group should have Read Only Permissions' {
        #    $ReaderGroup.Count | Should -BeExactly 1
        #}
    }

    Context 'SilverGold Zone Storage Account' {
        It 'Storage Account should be Gen 2' {
            $SilverGoldStorageAccount.Kind | Should -Be "StorageV2"
        }
        It 'Storage Account should use Hot Access Tier' {
            $SilverGoldStorageAccount.AccessTier | Should -Be "Hot"
        }
        It 'Storage Account should only support Https Traffic' {
            $SilverGoldStorageAccount.EnableHttpsTrafficOnly | Should -Be "true"
        }
        It 'SkuName should be Standard_LRS' {
            $SilverGoldStorageAccount.Sku.Name | Should -Be "Standard_LRS"
        }
        It 'Tier should be Standard' {
            $SilverGoldStorageAccount.Sku.Tier | Should -Be "Standard"
        }
        It 'NetworkRuleSet should Bypass AzureServices' {
            $SilverGoldStorageAccount.NetworkRuleSet.Bypass | Should -Be "AzureServices"
        }
        It 'NetworkRuleSet Default Action should be deny' {
            $SilverGoldStorageAccount.NetworkRuleSet.DefaultAction | Should -Be "Deny"
        }
        It 'NetworkRuleSet should contain private and public subnets.' {
            $SilverGoldStorageAccount.NetworkRuleSet.VirtualNetworkRules.VirtualNetworkResourceId | Should -HaveCount 2
        }
        #It 'Storage Account has a file system named goldprotected' {
        #    $FileSystems[0].Name | Should -Be "goldprotected"
        #}
        #It 'Storage Account has a file system named goldgeneral' {
        #    $FileSystems[0].Name | Should -Be "goldgeneral"
        #}
        #It 'Storage Account has a file system named silverprotected' {
        #    $FileSystems[0].Name | Should -Be "silverprotected"
        #}
        #It 'Storage Account has a file system named silvergeneral' {
        #    $FileSystems[0].Name | Should -Be "silvergeneral"
        #}
        #It 'framework File System does not allow public access' {
        #    $FileSystems[0].PublicAccess | Should -Be "Off"
        #}
        #It 'Default Scope Permissions should be set for superuser and 3 AD Groups' {
        #    $DefaultScopePermissions.Count | Should -BeGreaterOrEqual 4
        #}
        #It 'Reader Group should have Read Only Permissions' {
        #    $ReaderGroup.Count | Should -BeExactly 1
        #}
    }

    Context 'Sandbox Zone Storage Account' {
        It 'Storage Account should be Gen 2' {
            $SandboxStorageAccount.Kind | Should -Be "StorageV2"
        }
        It 'Storage Account should use Hot Access Tier' {
            $SandboxStorageAccount.AccessTier | Should -Be "Hot"
        }
        It 'Storage Account should only support Https Traffic' {
            $SandboxStorageAccount.EnableHttpsTrafficOnly | Should -Be "true"
        }
        It 'SkuName should be Standard_LRS' {
            $SandboxStorageAccount.Sku.Name | Should -Be "Standard_LRS"
        }
        It 'Tier should be Standard' {
            $SandboxStorageAccount.Sku.Tier | Should -Be "Standard"
        }
        It 'NetworkRuleSet should Bypass AzureServices' {
            $SandboxStorageAccount.NetworkRuleSet.Bypass | Should -Be "AzureServices"
        }
        It 'NetworkRuleSet Default Action should be deny' {
            $SandboxStorageAccount.NetworkRuleSet.DefaultAction | Should -Be "Deny"
        }
        It 'NetworkRuleSet should contain private and public subnets.' {
            $SandboxStorageAccount.NetworkRuleSet.VirtualNetworkRules.VirtualNetworkResourceId | Should -HaveCount 2
        }
        #It 'Storage Account has a file system named defaultsandbox' {
        #    $FileSystems[0].Name | Should -Be "defaultsandbox"
        #}
        #It 'framework File System does not allow public access' {
        #    $FileSystems[0].PublicAccess | Should -Be "Off"
        #}
        #It 'Default Scope Permissions should be set for superuser and 3 AD Groups' {
        #    $DefaultScopePermissions.Count | Should -BeGreaterOrEqual 4
        #}
        #It 'Reader Group should have Read Only Permissions' {
        #    $ReaderGroup.Count | Should -BeExactly 1
        #}
    }
}