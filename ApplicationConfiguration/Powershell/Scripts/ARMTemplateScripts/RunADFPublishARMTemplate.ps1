param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$Location,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$DataFactoryName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$ADFPublishARMTemplateFile,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$ADLSGen2StorageAccountName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$BlobStorageAccountName,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$KeyVaultName,
    [Parameter(Mandatory = $false, Position = 9)]
    [string]$LocalFileSystemHost = "c:\tmp",
    [Parameter(Mandatory = $false, Position = 10)]
    [string]$LocalFileSystemUserName = "bogus",
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$DatabricksToken
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1
. $ModulesDirectory\Databricks\DatabricksInstancePools.ps1

$Token = ConvertTo-SecureString -String $DatabricksToken -AsPlainText -Force
$DatabricksWorkspace = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName
[string]$DatabricksWorkspaceUrl = $DatabricksWorkspace.Url
#temporary workaround for secret not saving appropriately to key vault--just get the workspaceId instead
#[string]$DatabricksWorkspaceUrl = (Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name "DatabricksWorkspaceUrl").SecretValueText
[string]$Machine = "https://$DatabricksWorkspaceUrl"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
[string]$DatabricksClusterPoolId = (((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools | Where-Object {$_.instance_pool_name -eq "Default"}).instance_pool_id
[string]$DatabricksClusterVersion = "7.2.x-scala2.12"
[string]$DatabricksWorkers = "1"

$ADFPublishTemplateParameterObject = @{
    'factoryName' = "$DataFactoryName";
    'Internal ADLS Gen2_properties_typeProperties_url' = "https://$ADLSGen2StorageAccountName.dfs.core.windows.net";
    'Internal Blob Storage_properties_typeProperties_serviceEndpoint' = "https://$BlobStorageAccountName.blob.core.windows.net";
    'Internal Framework Database_properties_typeProperties_connectionString_secretName' = "FrameworkDBConnectionString";
    'Internal Key Vault_properties_typeProperties_baseUrl' = "https://$KeyVaultName.vault.azure.net/";
    'Local File System_properties_typeProperties_host' = "$LocalFileSystemHost";
    'Local File System_properties_typeProperties_userId' = "$LocalFileSystemUserName";
    'dataFactory_location' = "$Location";
    'DatabricksClusterVersion' = "$DatabricksClusterVersion";
    'DatabricksClusterPoolId' = "$DatabricksClusterPoolId";
    'DatabricksWorkers' = "$DatabricksWorkers";
    'DatabricksWorkspaceUrl' = "$Machine";
}

New-ResourceGroupDeployment -ResourceGroupName $ResourceGroupName -TemplateFile $ADFPublishARMTemplateFile -TemplateParameterObject $ADFPublishTemplateParameterObject -Mode "Incremental"
