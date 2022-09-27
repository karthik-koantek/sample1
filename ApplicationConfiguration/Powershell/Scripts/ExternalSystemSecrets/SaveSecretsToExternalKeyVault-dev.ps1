param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ScriptsDirectory,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory
)

Set-AzContext -SubscriptionId $SubscriptionId

#Note: this script wouldn't normally be run as part of the ci/cd pipeline but is included here for instructional and documentation purposes.
# normally secrets would be assembled outside of the pipeline and wouldn't be stored in source control.
[string]$KeyVaultName = "ktk-d-mdp-ext-kv"
[string]$ExternalSQLServerName = "ktkvdc.database.windows.net"
[string]$ExternalDatabaseName = "ktkvdc"
[string]$ExternalDatabaseLogin = "ktkvdc"
[string]$ExternalDatabasePwd = "BiN3ReTNK3Z8rzw"
[string]$TwitterIngestionEventHubConnectionString = "Endpoint=sb://twitterapi.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8rcqXT7DBiV3IDeyOJL7zU4OAJfidkXczfiS3FmqbT0="
[string]$TwitterIngestionEventHubName = "twitteringestion"
[string]$TwitterIngestionEventHubConsumerGroup = "`$Default"
[string]$TwitterDistributionEventHubConnectionString = "Endpoint=sb://twitterapi.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=8rcqXT7DBiV3IDeyOJL7zU4OAJfidkXczfiS3FmqbT0="
[string]$TwitterDistributionEventHubName = "twitterdistribution"
[string]$TwitterDistributionEventHubConsumerGroup = "`$Default"
[string]$TwitterAPIKey = "2nYIe1C7O7zrNJBMbeIHgc73r"
[string]$TwitterSecretKey = "a5PukVdfC3ssJMniY2wJFQptQMFcbY8i8zXoGhjETwG0z4r4lH"
[string]$TwitterAccessToken = "24121201-8RQixiZ0MrAGNKgahCsXJ4LIDFb8xF7U6PjI9uaeo"
[string]$TwitterAccessTokenSecret = "4Ei567TcRLRkLFR5mUpeVJOnSIA59bkEecWGhjFlXUME2"
[string]$CognitiveServicesAccessKey = "342ccceb5bfe4dadb4f2829581c396f4"
[string]$CognitiveServicesEndpoint = "https://westus2.api.cognitive.microsoft.com"
[string]$CVIngestionEventHubConnectionString = "Endpoint=sb://dvc-p-mdp-cv-evt.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=KJV7g+d5ca1XUfV3xOQyb2zaJ+sCuT7V/WSymCCYakU="
[string]$CVIngestionEventHubName = "cvingest"
[string]$CVIngestionEventHubConsumerGroup = "`$Default"
[string]$CVDistributionEventHubConnectionString = "Endpoint=sb://dvc-p-mdp-cv-evt.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=KJV7g+d5ca1XUfV3xOQyb2zaJ+sCuT7V/WSymCCYakU="
[string]$CVDistributionEventHubName = "cvdistribution"
[string]$CVDistributionEventHubConsumerGroup = "`$Default"
[string]$ADXApplicationId = "72a91c37-83c5-4200-b667-4dcb02da719d"
[string]$ADXApplicationKey = ".0q._8.ReDyFgbWJj8-D6q~yv0m4DoTNOw"
[string]$ADXApplicationAuthorityId = "5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd"
[string]$ADXClusterName = "dvcpmdpcvadx.westus2"
[string]$CosmosDBEndPoint = "https://dvc-p-mdp-cv-cdb.documents.azure.com:443/"
[string]$CosmosDBMasterKey = "5O2J8OO5eoBKh5cQshKobb0KfrjHcIudkm7naZyMcWCh5KDgyokKDU2sJ2ILAtPRvU7KZSCl4tgqwhUEzBrQdQ=="
[string]$CosmosDBDatabase = "dvcatalyst"
[string]$CosmosDBPreferredRegions = "westus2"
[string]$ExternalStorageAccountName = "dvcpmdpcvsa"
[string]$ExternalStorageAccountKey = "+OLk+nxxc6gPtY36Ch2fwRyBLvmFsMzuW5gomxnOHOxO2ixKhTl0hpW7I6Zl3YiwtGXgnyfCjYOu/PJapxdYKw=="
[string]$ExternalContainerOrFileSystemName = "dvcatalystexternaldata"
[string]$ExternalStorageAccountType = "blob"
[string]$ScriptPath = "$ScriptsDirectory\ExternalSystemSecrets\SaveSecretsToExternalKeyVault.ps1"
[string]$ExternalFileSystemHost = "C:\ADFSampleFiles"
[string]$ExternalFileSystemUserName = "adf"
[string]$ExternalFileSystemPwd = "2s)Ng[8mxr4wCnvC"

& $ScriptPath `
    -SubscriptionId $SubscriptionId `
    -KeyVaultName $KeyVaultName `
    -ModulesDirectory $ModulesDirectory `
    -ExternalSQLServerName $ExternalSQLServerName `
    -ExternalDatabaseName $ExternalDatabaseName `
    -ExternalDatabaseLogin $ExternalDatabaseLogin `
    -ExternalDatabasePwd $ExternalDatabasePwd `
    -TwitterIngestionEventHubConnectionString $TwitterIngestionEventHubConnectionString `
    -TwitterIngestionEventHubName $TwitterIngestionEventHubName `
    -TwitterIngestionEventHubConsumerGroup $TwitterIngestionEventHubConsumerGroup `
    -TwitterDistributionEventHubConnectionString $TwitterDistributionEventHubConnectionString `
    -TwitterDistributionEventHubName $TwitterDistributionEventHubName `
    -TwitterDistributionEventHubConsumerGroup $TwitterDistributionEventHubConsumerGroup `
    -TwitterAPIKey $TwitterAPIKey `
    -TwitterSecretKey $TwitterSecretKey `
    -TwitterAccessToken $TwitterAccessToken `
    -TwitterAccessTokenSecret $TwitterAccessTokenSecret `
    -CognitiveServicesAccessKey $CognitiveServicesAccessKey `
    -CognitiveServicesEndpoint $CognitiveServicesEndpoint `
    -CVIngestionEventHubConnectionString $CVIngestionEventHubConnectionString `
    -CVIngestionEventHubName $CVIngestionEventHubName `
    -CVIngestionEventHubConsumerGroup $CVIngestionEventHubConsumerGroup `
    -CVDistributionEventHubConnectionString $CVDistributionEventHubConnectionString `
    -CVDistributionEventHubName $CVDistributionEventHubName `
    -CVDistributionEventHubConsumerGroup $CVDistributionEventHubConsumerGroup `
    -ADXApplicationId $ADXApplicationId `
    -ADXApplicationKey $ADXApplicationKey `
    -ADXApplicationAuthorityId $ADXApplicationAuthorityId `
    -ADXClusterName $ADXClusterName `
    -CosmosDBEndPoint $CosmosDBEndPoint `
    -CosmosDBMasterKey $CosmosDBMasterKey `
    -CosmosDBDatabase $CosmosDBDatabase `
    -CosmosDBPreferredRegions $CosmosDBPreferredRegions `
    -ExternalStorageAccountName $ExternalStorageAccountName `
    -ExternalStorageAccountKey $ExternalStorageAccountKey `
    -ExternalContainerOrFileSystemName $ExternalContainerOrFileSystemName `
    -ExternalStorageAccountType $ExternalStorageAccountType `
    -ExternalFileSystemHost $ExternalFileSystemHost `
    -ExternalFileSystemUserName $ExternalFileSystemUserName `
    -ExternalFileSystemPwd $ExternalFileSystemPwd

