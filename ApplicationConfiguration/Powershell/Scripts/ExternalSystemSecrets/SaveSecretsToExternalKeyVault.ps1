param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$KeyVaultName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ExternalSQLServerName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$ExternalDatabaseName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$ExternalDatabaseLogin,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$ExternalDatabasePwd,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$TwitterIngestionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$TwitterIngestionEventHubName,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$TwitterIngestionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$TwitterDistributionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$TwitterDistributionEventHubName,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$TwitterDistributionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 13)]
    [string]$TwitterAPIKey,
    [Parameter(Mandatory = $true, Position = 14)]
    [string]$TwitterSecretKey,
    [Parameter(Mandatory = $true, Position = 15)]
    [string]$TwitterAccessToken,
    [Parameter(Mandatory = $true, Position = 16)]
    [string]$TwitterAccessTokenSecret,
    [Parameter(Mandatory = $true, Position = 17)]
    [string]$CognitiveServicesAccessKey,
    [Parameter(Mandatory = $true, Position = 18)]
    [string]$CognitiveServicesEndpoint,
    [Parameter(Mandatory = $true, Position = 19)]
    [string]$CVIngestionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 20)]
    [string]$CVIngestionEventHubName,
    [Parameter(Mandatory = $true, Position = 21)]
    [string]$CVIngestionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 22)]
    [string]$CVDistributionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 23)]
    [string]$CVDistributionEventHubName,
    [Parameter(Mandatory = $true, Position = 24)]
    [string]$CVDistributionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 25)]
    [string]$ADXApplicationId,
    [Parameter(Mandatory = $true, Position = 26)]
    [string]$ADXApplicationKey,
    [Parameter(Mandatory = $true, Position = 27)]
    [string]$ADXApplicationAuthorityId,
    [Parameter(Mandatory = $true, Position = 28)]
    [string]$ADXClusterName,
    [Parameter(Mandatory = $true, Position = 29)]
    [string]$CosmosDBEndPoint,
    [Parameter(Mandatory = $true, Position = 30)]
    [string]$CosmosDBMasterKey,
    [Parameter(Mandatory = $true, Position = 31)]
    [string]$CosmosDBDatabase,
    [Parameter(Mandatory = $true, Position = 32)]
    [string]$CosmosDBPreferredRegions,
    [Parameter(Mandatory = $true, Position = 33)]
    [string]$ExternalStorageAccountName,
    [Parameter(Mandatory = $true, Position = 34)]
    [string]$ExternalStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 35)]
    [string]$ExternalContainerOrFileSystemName,
    [Parameter(Mandatory = $true, Position = 36)]
    [string]$ExternalStorageAccountType,
    [Parameter(Mandatory = $true, Position = 37)]
    [string]$ExternalFileSystemUserName,
    [Parameter(Mandatory = $true, Position = 38)]
    [string]$ExternalFileSystemPwd,
    [Parameter(Mandatory = $true, Position = 39)]
    [string]$ExternalFileSystemHost
)

Set-AzContext -SubscriptionId $SubscriptionId
. $ModulesDirectory\KeyVault\KeyVaultModule.ps1

Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalSQLServerName" -SecretValue $ExternalSQLServerName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabaseName" -SecretValue $ExternalDatabaseName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabaseLogin" -SecretValue $ExternalDatabaseLogin
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalDatabasePwd" -SecretValue $ExternalDatabasePwd
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterIngestionEventHubConnectionString" -SecretValue $TwitterIngestionEventHubConnectionString
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterIngestionEventHubName" -SecretValue $TwitterIngestionEventHubName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterIngestionEventHubConsumerGroup" -SecretValue $TwitterIngestionEventHubConsumerGroup
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterDistributionEventHubConnectionString" -SecretValue $TwitterDistributionEventHubConnectionString
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterDistributionEventHubName" -SecretValue $TwitterDistributionEventHubName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterDistributionEventHubConsumerGroup" -SecretValue $TwitterDistributionEventHubConsumerGroup
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterAPIKey" -SecretValue $TwitterAPIKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterSecretKey" -SecretValue $TwitterSecretKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterAccessToken" -SecretValue $TwitterAccessToken
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "TwitterAccessTokenSecret" -SecretValue $TwitterAccessTokenSecret
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CognitiveServicesAccessKey" -SecretValue $CognitiveServicesAccessKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CognitiveServicesEndpoint" -SecretValue $CognitiveServicesEndpoint
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVIngestionEventHubConnectionString" -SecretValue $CVIngestionEventHubConnectionString
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVIngestionEventHubName" -SecretValue $CVIngestionEventHubName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVIngestionEventHubConsumerGroup" -SecretValue $CVIngestionEventHubConsumerGroup
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVDistributionEventHubConnectionString" -SecretValue $CVDistributionEventHubConnectionString
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVDistributionEventHubName" -SecretValue $CVDistributionEventHubName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CVDistributionEventHubConsumerGroup" -SecretValue $CVDistributionEventHubConsumerGroup
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADXApplicationId" -SecretValue $ADXApplicationId
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADXApplicationKey" -SecretValue $ADXApplicationKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADXApplicationAuthorityId" -SecretValue $ADXApplicationAuthorityId
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ADXClusterName" -SecretValue $ADXClusterName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CosmosDBEndPoint" -SecretValue $CosmosDBEndPoint
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CosmosDBMasterKey" -SecretValue $CosmosDBMasterKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CosmosDBDatabase" -SecretValue $CosmosDBDatabase
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "CosmosDBPreferredRegions" -SecretValue $CosmosDBPreferredRegions
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountName" -SecretValue $ExternalStorageAccountName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountKey" -SecretValue $ExternalStorageAccountKey
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalContainerOrFileSystemName" -SecretValue $ExternalContainerOrFileSystemName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalStorageAccountType" -SecretValue $ExternalStorageAccountType
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemUserName" -SecretValue $ExternalFileSystemUserName
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemPwd" -SecretValue $ExternalFileSystemPwd
Set-KeyVaultSecret -KeyVaultName $KeyVaultName -SecretName "ExternalFileSystemHost" -SecretValue $ExternalFileSystemHost
