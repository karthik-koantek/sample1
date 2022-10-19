param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$ExternalKeyVaultSubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ExternalKeyVaultResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ExternalKeyVaultName,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$ExternalKeyVaultTenantId,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DevOpsServicePrincipalName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$DevOpsServicePrincipalSecret,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$DevOpsServicePrincipalApplicationId,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$Region,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$BearerToken,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$ScriptsDirectory,
    [Parameter(Mandatory = $true, Position = 13)]
    [string]$KeyVaultName,
    [Parameter(Mandatory = $true, Position = 14)]
    [string]$FrameworkDBAdminPwd,
    [Parameter(Mandatory = $false, Position = 15)]
    [string]$SanctionedDatabaseName = "",
    [Parameter(Mandatory = $false, Position = 16)]
    [string]$ReadSecretsFromExternalKeyVault = "false",
    [Parameter(Mandatory = $false, Position = 17)]
    [string]$DeployUATTesting = "false"
)

Set-AzContext -SubscriptionId $SubscriptionId
Install-Module -Name Az.Databricks -Force

$DatabricksWorkspace = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName
[string]$DatabricksWorkspaceUrl = $DatabricksWorkspace.Url
[string]$Machine = "https://$DatabricksWorkspaceUrl"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1
. $ModulesDirectory\Databricks\DatabricksTokens.ps1
. $ModulesDirectory\KeyVault\KeyVaultModule.ps1

if ($ReadSecretsFromExternalKeyVault -eq "true" -or $DeployUATTesting -eq "true") {
    . $ScriptsDirectory\ExternalSystemSecrets\ReadSecretsFromExternalKeyVault.ps1 -ExternalKeyVaultSubscriptionId $ExternalKeyVaultSubscriptionId -ExternalKeyVaultResourceGroupName $ExternalKeyVaultResourceGroupName -ExternalKeyVaultName $ExternalKeyVaultName -ModulesDirectory $ModulesDirectory -ExternalKeyVaultTenantId $ExternalKeyVaultTenantId -DevOpsServicePrincipalName $DevOpsServicePrincipalName -DevOpsServicePrincipalSecret $DevOpsServicePrincipalSecret -DevOpsServicePrincipalApplicationId $DevOpsServicePrincipalApplicationId
}

#region begin DatabricksSecretScopes

#casella external blob storage
Write-Host "Databricks Secret Scope CasellaExternalBlobStore" -ForegroundColor Cyan
& $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForStorageAccount.ps1 `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Region $Region `
    -BearerToken $BearerToken `
    -ModulesDirectory $ModulesDirectory `
    -ExternalSystem "CasellaExternalBlobStore" `
    -StorageAccountName $CasellaStorageAccountName `
    -StorageAccountKey $CasellaStorageAccountKey `
    -ContainerOrFileSystemName $CasellaContainerOrFileSystemName `
    -StorageAccountType $CasellaStorageAccountType 

if ($ReadSecretsFromExternalKeyVault -eq "true" -and $DeployUATTesting -eq "true") {
    #adventureworkslt
    Write-Host "Databricks Secret Scope adventureworkslt" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForSQL `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "adventureworkslt" `
        -SQLServerName $ExternalSQLServerName `
        -DatabaseName $ExternalDatabaseName `
        -Login $ExternalDatabaseLogin `
        -Pwd $ExternalDatabasePwd

    #twitter event hub ingestion
    Write-Host "Databricks Secret Scope EventHubTwitterIngestion" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForEventHub.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "EventHubTwitterIngestion" `
        -EventHubConnectionString $TwitterIngestionEventHubConnectionString `
        -EventHubName $TwitterIngestionEventHubName `
        -EventHubConsumerGroup $TwitterIngestionEventHubConsumerGroup

    #twitter event hub distribution
    Write-Host "Databricks Secret Scope EventHubTwitterDistribution" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForEventHub.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "EventHubTwitterDistribution" `
        -EventHubConnectionString $TwitterDistributionEventHubConnectionString `
        -EventHubName $TwitterDistributionEventHubName `
        -EventHubConsumerGroup $TwitterDistributionEventHubConsumerGroup

    #twitter api
    Write-Host "Databricks Secret Scope TwitterAPI" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForTwitterAPI.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "TwitterAPI" `
        -APIKey $TwitterAPIKey `
        -SecretKey $TwitterSecretKey `
        -AccessToken $TwitterAccessToken `
        -AccessTokenSecret $TwitterAccessTokenSecret

    #cognitive services
    Write-Host "Databricks Secret Scope CognitiveServices" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForAzureCognitiveServices.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "CognitiveServices" `
        -AccessKey $CognitiveServicesAccessKey `
        -Endpoint $CognitiveServicesEndpoint

    #cv event hub ingestion
    Write-Host "Databricks Secret Scope EventHubCVIngestion" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForEventHub.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "EventHubCVIngestion" `
        -EventHubConnectionString $CVIngestionEventHubConnectionString `
        -EventHubName $CVIngestionEventHubName `
        -EventHubConsumerGroup $CVIngestionEventHubConsumerGroup

    #cv event hub distribution
    Write-Host "Databricks Secret Scope EventHubCVDistribution" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForEventHub.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "EventHubCVDistribution" `
        -EventHubConnectionString $CVDistributionEventHubConnectionString `
        -EventHubName $CVDistributionEventHubName `
        -EventHubConsumerGroup $CVDistributionEventHubConsumerGroup

    #cv azure data explorer
    Write-Host "Databricks Secret Scope AzureDataExplorer" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForAzureDataExplorer.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "AzureDataExplorer" `
        -ADXApplicationId $ADXApplicationId `
        -ADXApplicationKey $ADXApplicationKey `
        -ADXApplicationAuthorityId $ADXApplicationAuthorityId `
        -ADXClusterName $ADXClusterName

    #cosmos db
    Write-Host "Databricks Secret Scope CosmosDB" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForCosmosDB.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "CosmosDB" `
        -EndPoint $CosmosDBEndPoint `
        -MasterKey $CosmosDBMasterKey `
        -Database $CosmosDBDatabase `
        -PreferredRegions $CosmosDBPreferredRegions

    #external blob storage
    Write-Host "Databricks Secret Scope ExternalBlobStore" -ForegroundColor Cyan
    & $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForStorageAccount.ps1 `
        -SubscriptionId $SubscriptionId `
        -ResourceGroupName $ResourceGroupName `
        -Region $Region `
        -BearerToken $BearerToken `
        -ModulesDirectory $ModulesDirectory `
        -ExternalSystem "ExternalBlobStore" `
        -StorageAccountName $ExternalStorageAccountName `
        -StorageAccountKey $ExternalStorageAccountKey `
        -ContainerOrFileSystemName $ExternalContainerOrFileSystemName `
        -StorageAccountType $ExternalStorageAccountType
}

#endregion

#region begin KeyVaultSecets

#parameters to add

#Databricks Workspace Id
Write-Host "Key Vault Secret DatabricksWorkspaceId" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "DatabricksWorkspaceId" -SecretValue $DatabricksWorkspace.WorkspaceId
Write-Host "Key Vault Secret DatabricksWorkspaceUrl" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "DatabricksWorkspaceUrl" -SecretValue $DatabricksWorkspaceUrl

#ADF Databricks Token
Write-Host "Key Vault Secret ADFDatabricksToken" -ForegroundColor Cyan
Write-Host "URIBase: $URIBase"
$ADFBearerToken = Get-DatabricksToken -BaseURI $URIBase -Token $Token -Comment "adf"
Revoke-DatabricksToken -BaseURI $URIBase -Token $Token -TokenIdToDelete $ADFBearerToken.token_id
$ADFBearerToken = (((New-DatabricksToken -BaseURI $URIBase -Token $Token -Comment "adf").Content) | ConvertFrom-Json).token_value
if($ADFBearerToken) {Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADFDatabricksToken" -SecretValue $ADFBearerToken}

#Storage Accounts
Remove-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADLSStorageAccountName"
Remove-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADLSStorageAccountKey"
Remove-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "BlobStorageAccountName"
Remove-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "BlobStorageAccountKey"
$StorageAccounts = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName
$TransientStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*blobt"}
$BronzeStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlsb"}
$SilverGoldStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssg"}
$SandboxStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssb"}
$TransientStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $TransientStorageAccount.StorageAccountName).Value[0]
$BronzeStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $BronzeStorageAccount.StorageAccountName).Value[0]
$SilverGoldStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $SilverGoldStorageAccount.StorageAccountName).Value[0]
$SandboxStorageAccountKey = (Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $SandboxStorageAccount.StorageAccountName).Value[0]
Write-Host "Key Vault Secret TransientStorageAccountName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TransientStorageAccountName" -SecretValue $TransientStorageAccount.StorageAccountName
Write-Host "Key Vault Secret TransientStorageAccountKey" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TransientStorageAccountKey" -SecretValue $TransientStorageAccountKey
Write-Host "Key Vault Secret BronzeStorageAccountName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "BronzeStorageAccountName" -SecretValue $BronzeStorageAccount.StorageAccountName
Write-Host "Key Vault Secret BronzeStorageAccountKey" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "BronzeStorageAccountKey" -SecretValue $BronzeStorageAccountKey
Write-Host "Key Vault Secret SilverGoldStorageAccountName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SilverGoldStorageAccountName" -SecretValue $SilverGoldStorageAccount.StorageAccountName
Write-Host "Key Vault Secret SilverGoldStorageAccountKey" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SilverGoldStorageAccountKey" -SecretValue $SilverGoldStorageAccountKey
Write-Host "Key Vault Secret SandboxStorageAccountName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SandboxStorageAccountName" -SecretValue $SandboxStorageAccount.StorageAccountName
Write-Host "Key Vault Secret SandboxStorageAccountKey" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SandboxStorageAccountKey" -SecretValue $SandboxStorageAccountKey

#SQL Metadata DB
$SQLServers = Get-AzSqlServer -ResourceGroupName $ResourceGroupName
Write-Host "Key Vault Secret FrameworkDBServerName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "FrameworkDBServerName" -SecretValue $SQLServers.FullyQualifiedDomainName
Write-Host "Key Vault Secret FrameworkDBAdministratorLogin" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "FrameworkDBAdministratorLogin" -SecretValue $SQLServers.SqlAdministratorLogin
Write-Host "Key Vault Secret FrameworkDBAdministratorPwd" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "FrameworkDBAdministratorPwd" -SecretValue $FrameworkDBAdminPwd
$FrameworkDBConnectionString = "Server=tcp:" + $SQLServers.FullyQualifiedDomainName + ",1433;Initial Catalog=Accelerator;Persist Security Info=False;User ID=" + $SQLServers.SqlAdministratorLogin + ";Password=$FrameworkDBAdminPwd;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
Write-Host "Key Vault Secret FrameworkDBConnectionString" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "FrameworkDBConnectionString" -SecretValue $FrameworkDBConnectionString

#Data Warehouse DB
if ($SanctionedDatabaseName -ne "") {
    #Data Warehouse is optional, right now assume if it exists, it uses the same Azure SQL Server as the framework db.
    Write-Host "Key Vault Secret SanctionedSQLServerName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SanctionedSQLServerName" -SecretValue $SQLServers.FullyQualifiedDomainName
    Write-Host "Key Vault Secret SanctionedSQLServerLogin" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SanctionedSQLServerLogin" -SecretValue $SQLServers.SqlAdministratorLogin
    Write-Host "Key Vault Secret SanctionedSQLServerPwd" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SanctionedSQLServerPwd" -SecretValue $FrameworkDBAdminPwd
    Write-Host "Key Vault Secret SanctionedDatabaseName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SanctionedDatabaseName" -SecretValue $SanctionedDatabaseName
    Write-Host "Key Vault Secret ADFDatabricksToken" -ForegroundColor Cyan
    $SanctionedDBConnectionString = "Server=tcp:" + $SQLServers.FullyQualifiedDomainName + ",1433;Initial Catalog=$SanctionedDatabaseName;Persist Security Info=False;User ID=" + $SQLServers.SqlAdministratorLogin + ";Password=$FrameworkDBAdminPwd;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    Write-Host "Key Vault Secret SanctionedDatabaseConnectionString" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "SanctionedDatabaseConnectionString" -SecretValue $SanctionedDBConnectionString
}

#Casella External Storage Account
Write-Host "Key Vault Secret CasellaExternalStorageAccountName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CasellaExternalStorageAccountName" -SecretValue $CasellaStorageAccountName   
Write-Host "Key Vault Secret CasellaExternalStorageAccountKey" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CasellaExternalStorageAccountKey" -SecretValue $CasellaStorageAccountKey 
Write-Host "Key Vault Secret CasellaExternalContainerOrFileSystemName" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CasellaExternalContainerOrFileSystemName" -SecretValue $CasellaContainerOrFileSystemName    
Write-Host "Key Vault Secret CasellaExternalStorageAccountType" -ForegroundColor Cyan
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CasellaExternalStorageAccountType" -SecretValue $CasellaStorageAccountType 

if ($ReadSecretsFromExternalKeyVault -eq "true" -and $DeployUATTesting -eq "true") {
    #External Storage Account
    Write-Host "Key Vault Secret ExternalStorageAccountName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountName" -SecretValue $ExternalStorageAccountName
    Write-Host "Key Vault Secret ExternalStorageAccountKey" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountKey" -SecretValue $ExternalStorageAccountKey
    Write-Host "Key Vault Secret ExternalContainerOrFileSystemName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalContainerOrFileSystemName" -SecretValue $ExternalContainerOrFileSystemName
    Write-Host "Key Vault Secret ExternalStorageAccountType" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountType" -SecretValue $ExternalStorageAccountType

    #External SQL Server
    Write-Host "Key Vault Secret ExternalSQLServerName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalSQLServerName" -SecretValue $ExternalSQLServerName
    Write-Host "Key Vault Secret ExternalDatabaseName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabaseName" -SecretValue $ExternalDatabaseName
    Write-Host "Key Vault Secret ExternalDatabaseLogin" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabaseLogin" -SecretValue $ExternalDatabaseLogin
    Write-Host "Key Vault Secret ExternalDatabasePwd" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabasePwd" -SecretValue $ExternalDatabasePwd
    #$ExternalDatabaseConnectionString = "Server=tcp:$ExternalSQLServerName,1433;Initial Catalog=$ExternalDatabaseName;Persist Security Info=False;User ID=$ExternalDatabaseLogin;Password=$ExternalDatabasePwd;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;"
    #Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabaseConnectionString" -SecretValue $ExternalDatabaseConnectionString

    #External File System (On-Premises)
    Write-Host "Key Vault Secret ExternalFileSystemUserName" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemUserName" -SecretValue $ExternalFileSystemUserName
    Write-Host "Key Vault Secret ExternalFileSystemPassword" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemPassword" -SecretValue $ExternalFileSystemPwd
    Write-Host "Key Vault Secret ExternalFileSystemHost" -ForegroundColor Cyan
    Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemHost" -SecretValue $ExternalFileSystemHost
}

#endregion
