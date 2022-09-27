param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$DatabricksWorkspaceName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$Location,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$StorageAccountName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$AzureSQLServerName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$AzureSQLDatabaseName,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$AzureSQLServerAdministratorLogin,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$AzureSQLServerAdministratorPwd,
    [Parameter(Mandatory = $false, Position = 10)]
    [string]$NsgName = "databricks-nsg",
    [Parameter(Mandatory = $false, Position = 11)]
    [string]$VnetName = "databricks-vnet",
    [Parameter(Mandatory = $false, Position = 12)]
    [string]$PrivateSubnetName = "private-subnet",
    [Parameter(Mandatory = $false, Position = 13)]
    [string]$PublicSubnetName = "public-subnet",
    [Parameter(Mandatory = $false, Position = 14)]
    [string]$PricingTier = "premium",
    [Parameter(Mandatory = $false, Position = 15)]
    [string]$VnetCidr = "10.179.0.0/16",
    [Parameter(Mandatory = $false, Position = 16)]
    [string]$PrivateSubnetCidr = "10.179.0.0/18",
    [Parameter(Mandatory = $false, Position = 17)]
    [string]$PublicSubnetCidr = "10.179.64.0/18",
    [Parameter(Mandatory = $false, Position = 18)]
    [string]$StorageAccountType = "Standard_LRS",
    [Parameter(Mandatory = $false, Position = 19)]
    [string]$StorageAccountKind = "StorageV2",
    [Parameter(Mandatory = $false, Position = 20)]
    [string]$StorageAccountAccessTier = "Hot",
    [Parameter(Mandatory = $false, Position = 21)]
    [bool]$StorageAccountSupportsHttpsTrafficOnly = $true,
    [Parameter(Mandatory = $false, Position = 22)]
    [string]$NetworkAclsBypass = "AzureServices",
    [Parameter(Mandatory = $false, Position = 23)]
    [string]$NetworkAclsDefaultAction = "Deny",
    [Parameter(Mandatory = $false, Position = 24)]
    [bool]$IsHnsEnabled = $true,
    [Parameter(Mandatory = $false, Position = 25)]
    [string]$IpAddressFirewallException = "70.185.39.36",
    [Parameter(Mandatory = $true, Position = 26)]
    [string]$CoreARMTemplateFile
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

Write-Host "Creating Resource Group" -ForegroundColor Cyan
New-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -Location $Location

$Resources = Get-AzResource -ResourceGroupName $ResourceGroupName
If(!$Resources) {
    Write-Host "Running Core ARM Template" -ForegroundColor Cyan
    $TemplateParameterObject = @{
        nsgName = "$NsgName";
        vnetName = "$VnetName";
        workspaceName = "$DatabricksWorkspaceName";
        privateSubnetName = "$PrivateSubnetName";
        publicSubnetName = "$PublicSubnetName";
        pricingTier = "$PricingTier";
        location = "$Location";
        vnetCidr = "$VnetCidr";
        privateSubnetCidr = "$PrivateSubnetCidr";
        publicSubnetCidr = "$PublicSubnetCidr";
        storageAccountName = "$StorageAccountName";
        accountType = "$StorageAccountType";
        kind = "$StorageAccountKind";
        accessTier = "$StorageAccountAccessTier";
        supportsHttpsTrafficOnly = $StorageAccountSupportsHttpsTrafficOnly;
        networkAclsBypass = "$NetworkAclsBypass";
        networkAclsDefaultAction = "$NetworkAclsDefaultAction";
        isHnsEnabled = $IsHnsEnabled;
        azureSqlServerName = "$AzureSQLServerName";
        azureSqlDatabaseName = "$AzureSQLDatabaseName";
        azureSqlServerAdministratorLogin = "$AzureSQLServerAdministratorLogin";
        azureSqlServerAdministratorPassword = "$AzureSQLServerAdministratorPwd";
        ipAddressFirewallException = "$IpAddressFirewallException";
    }
    New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $CoreARMTemplateFile -TemplateParameterObject $TemplateParameterObject -Mode "Incremental"
} else {
    Write-Host "Resource Group is not empty, skipping Core ARM Template"
}
