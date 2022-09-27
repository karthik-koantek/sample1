param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$KeyVaultARMTemplateFile,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$KeyVaultName,
    [Parameter(Mandatory = $false, Position = 5)]
    [string]$KeyVaultSku = "Standard",
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$TenantId,
    [Parameter(Mandatory = $false, Position = 7)]
    [bool]$EnabledForDeployment = $true,
    [Parameter(Mandatory = $false, Position = 8)]
    [bool]$EnabledForTemplateDeployment = $true,
    [Parameter(Mandatory = $false, Position = 9)]
    [bool]$EnabledForDiskEncryption = $true,
    [Parameter(Mandatory = $false, Position = 10)]
    [bool]$EnableRbacAuthorization = $false,
    [Parameter(Mandatory = $false, Position = 11)]
    [bool]$EnableSoftDelete = $true,
    [Parameter(Mandatory = $false, Position = 12)]
    [int32]$SoftDeleteRetentionInDays = 30,
    [Parameter(Mandatory = $true, Position = 13)]
    [string]$DevOpsServicePrincipalName
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1
. $ModulesDirectory\AAD\AADSecurityGroupModule.ps1

Write-Host "Creating Resource Group" -ForegroundColor Cyan
New-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -Location $Location

$Resource = Get-AzResource -ResourceGroupName $ResourceGroupName -Name $KeyVaultName
if (!$Resource) {
    #incremental deployment would undo security settings, which have to be run manually due to insufficient permissions, so only do this once
    Write-Host "Running Key Vault ARM Template" -ForegroundColor Cyan
    $KeyVaultTemplateParameterObject = @{
        name = "$KeyVaultName";
        location = "$Location";
        sku = "$KeyVaultSku";
        accessPolicies = @();
        tenant = "$TenantId";
        enabledForDeployment = $EnabledForDeployment;
        enabledForTemplateDeployment = $EnabledForTemplateDeployment;
        enabledForDiskEncryption = $EnabledForDiskEncryption;
        enableRbacAuthorization = $EnableRbacAuthorization;
        enableSoftDelete = $false;
        softDeleteRetentionInDays = $SoftDeleteRetentionInDays;
        networkAcls = @{};
    }
    New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $KeyVaultARMTemplateFile -TemplateParameterObject $KeyVaultTemplateParameterObject -Mode "Incremental"
} else {
    Write-Host "Key Vault exists, skipping"
}

# Permissions to the Key Vault must be set manually because the build agent doens't have sufficient permissions
#Write-Host "IAM Roles and Access Policies for Key Vault"
#$DataLakeOwnerGroupName = "$ResourceGroupName Data Lake Owners"
#$DataLakeOwnerGroup = Get-AADGroup -AADGroupDisplayName $DataLakeOwnerGroupName
#$KeyVaultName = (Get-AzKeyVault -ResourceGroupName $ResourceGroupName).VaultName
#$Scope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.KeyVault/vaults/$KeyVaultName"
#New-RoleAssignment -ObjectId $DataLakeOwnerGroup.Id -Scope $Scope -RoleDefinitionName "Owner"
#Set-AzKeyVaultAccessPolicy `
#    -VaultName $KeyVaultName `
#    -ResourceGroupName $ResourceGroupName `
#    -ObjectId $DataLakeOwnerGroup.Id `
#    -PermissionsToSecrets get,list,set,delete,backup,restore,recover,purge
#
#Set-AzKeyVaultAccessPolicy `
#    -VaultName $KeyVaultName `
#    -ResourceGroupName $ResourceGroupName `
#    -ServicePrincipalName $DevOpsServicePrincipalName `
#    -PermissionsToSecrets get,list,set,delete,backup,restore,recover,purge