Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

$ResourceGroup = Get-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName
$Resources = Get-AzResource -ResourceGroupName $ResourceGroupName
$ResourcesGrouped = $Resources.ResourceType | Group-Object
$NetworkIntentPolicies = $ResourcesGrouped | Where-Object {$_.Name -eq "Microsoft.Network/networkIntentPolicies"}
$Databases = $ResourcesGrouped | Where-Object {$_.Name -eq "Microsoft.Sql/servers/databases"}
$StorageAccounts = $ResourcesGrouped | Where-Object {$_.Name -eq "Microsoft.Storage/StorageAccounts"}

Describe 'Catalyst Resource Group' {
    Context 'Resources' {
        It 'Resource Group Exists' {
            $ResourceGroup | Should -Be $true
        }
        It 'Contains a Databricks Workspace' {
            "Microsoft.Databricks/workspaces" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains 2 Network Intent Policies' {
            $NetworkIntentPolicies.Count | Should -BeExactly 2
        }
        It 'Contains a Network Security Group' {
            "Microsoft.Network/networkSecurityGroups" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains a Virtual Network' {
            "Microsoft.Network/virtualNetworks" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains an Azure SQL Server' {
            "Microsoft.Sql/servers" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains Storage Accounts' {
            "Microsoft.Storage/storageAccounts" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains a Data Factory' {
            "Microsoft.DataFactory/factories" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains a Key Vault' {
            "Microsoft.KeyVault/vaults" | Should -BeIn $Resources.ResourceType
        }
        It 'Contains at least 4 Storage Accounts' {
            $StorageAccounts.Count | Should -BeGreaterOrEqual 4
        }
        It 'Contains at least 2 Databases' {
            $Databases.Count | Should -BeGreaterOrEqual 2
        }
    }
}