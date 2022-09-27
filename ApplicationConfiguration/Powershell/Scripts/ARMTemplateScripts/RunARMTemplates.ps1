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
    [string]$TransientStorageAccountName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$BronzeStorageAccountName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$SilverGoldStorageAccountName,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$SandboxStorageAccountName,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$AzureSQLServerName,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$AzureSQLDatabaseName,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$AzureSQLServerAdministratorLogin,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$AzureSQLServerAdministratorPwd,
    [Parameter(Mandatory = $false, Position = 13)]
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
    [string]$TemplateDirectory,
    [Parameter(Mandatory = $true, Position = 27)]
    [string]$ScriptsDirectory,
    [Parameter(Mandatory = $true, Position = 28)]
    [string]$KeyVaultName,
    [Parameter(Mandatory = $false, Position = 29)]
    [string]$KeyVaultSku = "Standard",
    [Parameter(Mandatory = $true, Position = 30)]
    [string]$TenantId,
    [Parameter(Mandatory = $false, Position = 31)]
    [bool]$EnabledForDeployment = $true,
    [Parameter(Mandatory = $false, Position = 32)]
    [bool]$EnabledForTemplateDeployment = $true,
    [Parameter(Mandatory = $false, Position = 33)]
    [bool]$EnabledForDiskEncryption = $true,
    [Parameter(Mandatory = $false, Position = 34)]
    [bool]$EnableRbacAuthorization = $false,
    [Parameter(Mandatory = $false, Position = 35)]
    [bool]$EnableSoftDelete = $true,
    [Parameter(Mandatory = $false, Position = 36)]
    [int32]$SoftDeleteRetentionInDays = 30,
    [Parameter(Mandatory = $true, Position = 37)]
    [string]$DevOpsServicePrincipalName,
    [Parameter(Mandatory = $true, Position = 39)]
    [string]$DataFactoryName
)

#region begin Core ARM Template

# Run Core ARM Template (RunArmTemplate.ps1 ~ Rename)
$CoreARMTemplateFile = "$TemplateDirectory\Core\armdeployment.json"
Write-Host "Running Core ARM Template" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunCoreARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -DatabricksWorkspaceName $DatabricksWorkspaceName `
    -Location $Location `
    -StorageAccountName $SilverGoldStorageAccountName `
    -AzureSQLServerName $AzureSQLServerName `
    -AzureSQLDatabaseName $AzureSQLDatabaseName `
    -AzureSQLServerAdministratorLogin $AzureSQLServerAdministratorLogin `
    -AzureSQLServerAdministratorPwd $AzureSQLServerAdministratorPwd `
    -NsgName $NsgName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -PricingTier $PricingTier `
    -VnetCidr $VnetCidr `
    -PrivateSubnetCidr $PrivateSubnetCidr `
    -PublicSubnetCidr $PublicSubnetCidr `
    -StorageAccountType "Standard_LRS" `
    -StorageAccountKind "StorageV2" `
    -StorageAccountAccessTier "Hot" `
    -IsHnsEnabled $true `
    -StorageAccountSupportsHttpsTrafficOnly $StorageAccountSupportsHttpsTrafficOnly `
    -NetworkAclsBypass $NetworkAclsBypass `
    -NetworkAclsDefaultAction $NetworkAclsDefaultAction `
    -IpAddressFirewallException $IpAddressFirewallException `
    -CoreARMTemplateFile $CoreARMTemplateFile
#endregion

#region begin Key Vault ARM Template

# Run Key Vault ARM Template (CreateKeyVault.ps1 ~ Rename)
$KeyVaultARMTemplateFile = "$TemplateDirectory\KeyVault\armdeployment.json"
Write-Host "Running Key Vault ARM Template" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunKeyVaultARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -KeyVaultARMTemplateFile $KeyVaultARMTemplateFile `
    -KeyVaultName $KeyVaultName `
    -KeyVaultSku $KeyVaultSku `
    -TenantId $TenantId `
    -EnabledForDeployment $EnabledForDeployment `
    -EnabledForTemplateDeployment $EnabledForTemplateDeployment `
    -EnabledForDiskEncryption $EnabledForDiskEncryption `
    -EnableRbacAuthorization $EnableRbacAuthorization `
    -EnableSoftDelete $EnableSoftDelete `
    -SoftDeleteRetentionInDays $SoftDeleteRetentionInDays `
    -DevOpsServicePrincipalName $DevOpsServicePrincipalName
#endregion

#region begin Transient Storage ARM Template
# Run Storage Account ARM Template (Transient Zone)
$StorageAccountARMTemplateFile = "$TemplateDirectory\Storage\armdeployment.json"
Write-Host "Running Storage Account ARM Template (Transient Zone)" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunStorageAccountARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Location $Location `
    -ModulesDirectory $ModulesDirectory `
    -StorageAccountName $TransientStorageAccountName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -StorageAccountARMTemplateFile $StorageAccountARMTemplateFile `
    -IpAddressFirewallException $IpAddressFirewallException `
    -StorageAccountSku "Standard_LRS" `
    -StorageAccountKind "BlobStorage" `
    -StorageAccountAccessTier "Hot" `
    -IsHnsEnabled $false
#endregion

#region begin Bronze Storage ARM Template
# Run Storage Account ARM Template (Bronze Zone)
$StorageAccountARMTemplateFile = "$TemplateDirectory\Storage\armdeployment.json"
Write-Host "Running Storage Account ARM Template (Bronze Zone)" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunStorageAccountARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Location $Location `
    -ModulesDirectory $ModulesDirectory `
    -StorageAccountName $BronzeStorageAccountName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -StorageAccountARMTemplateFile $StorageAccountARMTemplateFile `
    -IpAddressFirewallException $IpAddressFirewallException `
    -StorageAccountSku "Standard_LRS" `
    -StorageAccountKind "StorageV2" `
    -StorageAccountAccessTier "Hot" `
    -IsHnsEnabled $true
#endregion

#region begin Silver/Gold Storage ARM Template

# Run Storage Account ARM Template (SilverGold Zone)
$StorageAccountARMTemplateFile = "$TemplateDirectory\Storage\armdeployment.json"
Write-Host "Running Storage Account ARM Template (SilverGold Zone)" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunStorageAccountARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Location $Location `
    -ModulesDirectory $ModulesDirectory `
    -StorageAccountName $SilverGoldStorageAccountName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -StorageAccountARMTemplateFile $StorageAccountARMTemplateFile `
    -IpAddressFirewallException $IpAddressFirewallException `
    -StorageAccountSku "Standard_LRS" `
    -StorageAccountKind "StorageV2" `
    -StorageAccountAccessTier "Hot" `
    -IsHnsEnabled $true
#endregion

#region begin Sandbox Storage ARM Template

# Run Storage Account ARM Template (Sandbox Zone)
$StorageAccountARMTemplateFile = "$TemplateDirectory\Storage\armdeployment.json"
Write-Host "Running Storage Account ARM Template (Sandbox Zone)" -ForegroundColor Cyan
& "$ScriptsDirectory\ARMTemplateScripts\RunStorageAccountARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Location $Location `
    -ModulesDirectory $ModulesDirectory `
    -StorageAccountName $SandboxStorageAccountName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -StorageAccountARMTemplateFile $StorageAccountARMTemplateFile `
    -IpAddressFirewallException $IpAddressFirewallException `
    -StorageAccountSku "Standard_LRS" `
    -StorageAccountKind "StorageV2" `
    -StorageAccountAccessTier "Hot" `
    -IsHnsEnabled $true
#endregion

#region begin ADF ARM Template

# Run ADF ARM Template
$ADFARMTemplateFile = "$TemplateDirectory\ADF\armdeployment.json"
& "$ScriptsDirectory\ARMTemplateScripts\RunADFARMTemplate.ps1" `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -DataFactoryName $DataFactoryName `
    -Location $Location `
    -ADFARMTemplateFile $ADFARMTemplateFile

# Run Synapse ARM Template
#endregion
