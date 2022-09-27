param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$Region,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$BearerToken,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$ScriptsDirectory,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$SQLServerName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$DatabaseName,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$Login,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$Pwd,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$TwitterIngestionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$TwitterIngestionEventHubName,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$TwitterIngestionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 13)]
    [string]$TwitterDistributionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 14)]
    [string]$TwitterDistributionEventHubName,
    [Parameter(Mandatory = $true, Position = 15)]
    [string]$TwitterDistributionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 16)]
    [string]$TwitterAPIKey,
    [Parameter(Mandatory = $true, Position = 17)]
    [string]$TwitterSecretKey,
    [Parameter(Mandatory = $true, Position = 18)]
    [string]$TwitterAccessToken,
    [Parameter(Mandatory = $true, Position = 19)]
    [string]$TwitterAccessTokenSecret,
    [Parameter(Mandatory = $true, Position = 20)]
    [string]$CognitiveServicesAccessKey,
    [Parameter(Mandatory = $true, Position = 21)]
    [string]$CognitiveServicesEndpoint,
    [Parameter(Mandatory = $true, Position = 22)]
    [string]$CVIngestionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 23)]
    [string]$CVIngestionEventHubName,
    [Parameter(Mandatory = $true, Position = 24)]
    [string]$CVIngestionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 25)]
    [string]$CVDistributionEventHubConnectionString,
    [Parameter(Mandatory = $true, Position = 26)]
    [string]$CVDistributionEventHubName,
    [Parameter(Mandatory = $true, Position = 27)]
    [string]$CVDistributionEventHubConsumerGroup,
    [Parameter(Mandatory = $true, Position = 28)]
    [string]$ADXApplicationId,
    [Parameter(Mandatory = $true, Position = 29)]
    [string]$ADXApplicationKey,
    [Parameter(Mandatory = $true, Position = 30)]
    [string]$ADXApplicationAuthorityId,
    [Parameter(Mandatory = $true, Position = 31)]
    [string]$ADXClusterName,
    [Parameter(Mandatory = $true, Position = 32)]
    [string]$CosmosDBEndPoint,
    [Parameter(Mandatory = $true, Position = 33)]
    [string]$CosmosDBMasterKey,
    [Parameter(Mandatory = $true, Position = 34)]
    [string]$CosmosDBDatabase,
    [Parameter(Mandatory = $true, Position = 35)]
    [string]$CosmosDBPreferredRegions,
    [Parameter(Mandatory = $true, Position = 36)]
    [string]$ExternalStorageAccountName,
    [Parameter(Mandatory = $true, Position = 37)]
    [string]$ExternalStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 38)]
    [string]$ExternalContainerOrFileSystemName,
    [Parameter(Mandatory = $true, Position = 39)]
    [string]$ExternalStorageAccountType
)

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

#adventureworkslt
& $ScriptsDirectory\ExternalSystemSecrets\CreateExternalSecretsForSQL `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Region $Region `
    -BearerToken $BearerToken `
    -ModulesDirectory $ModulesDirectory `
    -ExternalSystem "adventureworkslt" `
    -SQLServerName $SQLServerName `
    -DatabaseName $DatabaseName `
    -Login $Login `
    -Pwd $Pwd

#twitter event hub ingestion
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