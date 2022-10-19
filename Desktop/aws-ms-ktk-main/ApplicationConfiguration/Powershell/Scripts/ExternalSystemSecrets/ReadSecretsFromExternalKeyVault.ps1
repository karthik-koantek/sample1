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
    [string]$DevOpsServicePrincipalApplicationId
)

Write-Host "DevOpsServicePrincipalName: $DevOpsServicePrincipalName" 
Write-Host "DevOpsServicePrincipalSecret: $DevOpsServicePrincipalSecret"
Write-Host "ExternalKeyVaultTenantId: $ExternalKeyVaultTenantId" 
Write-Host "ExternalKeyVaultSubscriptionId: $ExternalKeyVaultSubscriptionId" 
Write-Host "ExternalKeyVaultResourceGroupName: $ExternalKeyVaultResourceGroupName"

#Write-Host "DEBUG: Get-AzADServicePrincipal -DisplayName $DevOpsServicePrincipalName"
#$ServicePrincipal = Get-AzADServicePrincipal -DisplayName $DevOpsServicePrincipalName
Write-Host "DEBUG: ConvertTo-SecureString $DevOpsServicePrincipalSecret -AsPlainText"
$ServicePrincipalSecret = ConvertTo-SecureString $DevOpsServicePrincipalSecret -AsPlainText 
#$ApplicationId = $ServicePrincipal.ApplicationId
Write-Host "DEBUG: New-Object -TypeName System.Management.Automation.PSCredential($DevOpsServicePrincipalApplicationId, $ServicePrincipalSecret)"
$ServicePrincipalCredential = New-Object -TypeName System.Management.Automation.PSCredential($DevOpsServicePrincipalApplicationId, $ServicePrincipalSecret)
Write-Host "DEBUG: Connect-AzAccount -ServicePrincipal -Credential $ServicePrincipalCredential -Tenant $ExternalKeyVaultTenantId"
Connect-AzAccount -ServicePrincipal -Credential $ServicePrincipalCredential -Tenant $ExternalKeyVaultTenantId
Write-Host "DEBUG: Set-AzContext -SubscriptionId $ExternalKeyVaultSubscriptionId -Tenant $ExternalKeyVaultTenantId"
Set-AzContext -SubscriptionId $ExternalKeyVaultSubscriptionId -Tenant $ExternalKeyVaultTenantId
. $ModulesDirectory\KeyVault\KeyVaultModule.ps1

Write-Host "DEBUG: Get-AzResource -ResourceGroupName $ExternalKeyVaultResourceGroupName -Name $ExternalKeyVaultName"
$Resource = Get-AzResource -ResourceGroupName $ExternalKeyVaultResourceGroupName -Name $ExternalKeyVaultName

function Get-SecretsHelper {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$KeyName
    )
    return (Get-AzKeyVaultSecret -VaultName $ExternalKeyVaultName -Name $KeyName).SecretValue | ConvertFrom-SecureString -AsPlainText
}
#Creating the key vault is an outside of this deployment process.  It may not exist, so only read secrets if it does. 
if($Resource) {
    $ExternalSQLServerName = Get-SecretsHelper -KeyName "ExternalSQLServerName"
    $ExternalDatabaseName = Get-SecretsHelper -KeyName "ExternalDatabaseName"
    $ExternalDatabaseLogin = Get-SecretsHelper -KeyName "ExternalDatabaseLogin" 
    $ExternalDatabasePwd = Get-SecretsHelper -KeyName "ExternalDatabasePwd" 
    $TwitterIngestionEventHubConnectionString = Get-SecretsHelper -KeyName "TwitterIngestionEventHubConnectionString" 
    $TwitterIngestionEventHubName = Get-SecretsHelper -KeyName "TwitterIngestionEventHubName"
    $TwitterIngestionEventHubConsumerGroup = Get-SecretsHelper -KeyName "TwitterIngestionEventHubConsumerGroup" 
    $TwitterDistributionEventHubConnectionString = Get-SecretsHelper -KeyName "TwitterDistributionEventHubConnectionString" 
    $TwitterDistributionEventHubName = Get-SecretsHelper -KeyName "TwitterDistributionEventHubName" 
    $TwitterDistributionEventHubConsumerGroup = Get-SecretsHelper -KeyName "TwitterDistributionEventHubConsumerGroup" 
    $TwitterAPIKey = Get-SecretsHelper -KeyName "TwitterAPIKey" 
    $TwitterSecretKey = Get-SecretsHelper -KeyName "TwitterSecretKey" 
    $TwitterAccessToken = Get-SecretsHelper -KeyName "TwitterAccessToken" 
    $TwitterAccessTokenSecret = Get-SecretsHelper -KeyName "TwitterAccessTokenSecret" 
    $CognitiveServicesAccessKey = Get-SecretsHelper -KeyName "CognitiveServicesAccessKey" 
    $CognitiveServicesEndpoint = Get-SecretsHelper -KeyName "CognitiveServicesEndpoint" 
    $CVIngestionEventHubConnectionString = Get-SecretsHelper -KeyName "CVIngestionEventHubConnectionString" 
    $CVIngestionEventHubName = Get-SecretsHelper -KeyName "CVIngestionEventHubName" 
    $CVIngestionEventHubConsumerGroup = Get-SecretsHelper -KeyName "CVIngestionEventHubConsumerGroup" 
    $CVDistributionEventHubConnectionString = Get-SecretsHelper -KeyName "CVDistributionEventHubConnectionString" 
    $CVDistributionEventHubName = Get-SecretsHelper -KeyName "CVDistributionEventHubName" 
    $CVDistributionEventHubConsumerGroup = Get-SecretsHelper -KeyName "CVDistributionEventHubConsumerGroup" 
    $ADXApplicationId = Get-SecretsHelper -KeyName "ADXApplicationId" 
    $ADXApplicationKey = Get-SecretsHelper -KeyName "ADXApplicationKey" 
    $ADXApplicationAuthorityId  = Get-SecretsHelper -KeyName "ADXApplicationAuthorityId"    
    $ADXClusterName = Get-SecretsHelper -KeyName "ADXClusterName"     
    $CosmosDBEndPoint  = Get-SecretsHelper -KeyName "CosmosDBEndPoint" 
    $CosmosDBMasterKey = Get-SecretsHelper -KeyName "CosmosDBMasterKey"    
    $CosmosDBDatabase = Get-SecretsHelper -KeyName "CosmosDBDatabase" 
    $CosmosDBPreferredRegions = Get-SecretsHelper -KeyName "CosmosDBPreferredRegions"   
    $CasellaStorageAccountName = Get-SecretsHelper -KeyName "CasellaStorageAccountName"    
    $CasellaStorageAccountKey = Get-SecretsHelper -KeyName "CasellaStorageAccountKey"   
    $CasellaContainerOrFileSystemName = Get-SecretsHelper -KeyName "CasellaContainerOrFileSystemName"    
    $CasellaStorageAccountType = Get-SecretsHelper -KeyName "CasellaStorageAccountType" 
    $ExternalStorageAccountName = Get-SecretsHelper -KeyName "ExternalStorageAccountName"
    $ExternalStorageAccountKey = Get-SecretsHelper -KeyName "ExternalStorageAccountKey"
    $ExternalContainerOrFileSystemName = Get-SecretsHelper -KeyName "ExternalContainerOrFileSystemName"
    $ExternalStorageAccountType = Get-SecretsHelper -KeyName "ExternalStorageAccountType"
    $ExternalFileSystemUserName = Get-SecretsHelper -KeyName "ExternalFileSystemUserName" 
    $ExternalFileSystemPwd = Get-SecretsHelper -KeyName "ExternalFileSystemPassword"
    $ExternalFileSystemHost = Get-SecretsHelper -KeyName "ExternalFileSystemHost"

    Write-Host "##vso[task.setvariable variable=ExternalSQLServerName;isOutput=true]$ExternalSQLServerName";
    Write-Host "##vso[task.setvariable variable=ExternalDatabaseName;isOutput=true]$ExternalDatabaseName";
    Write-Host "##vso[task.setvariable variable=ExternalDatabaseLogin;isOutput=true]$ExternalDatabaseLogin";
    Write-Host "##vso[task.setvariable variable=ExternalDatabasePwd;isOutput=true]$ExternalDatabasePwd";
    Write-Host "##vso[task.setvariable variable=TwitterIngestionEventHubConnectionString;isOutput=true]$TwitterIngestionEventHubConnectionString";
    Write-Host "##vso[task.setvariable variable=TwitterIngestionEventHubName;isOutput=true]$TwitterIngestionEventHubName";
    Write-Host "##vso[task.setvariable variable=TwitterIngestionEventHubConsumerGroup;isOutput=true]$TwitterIngestionEventHubConsumerGroup";
    Write-Host "##vso[task.setvariable variable=TwitterDistributionEventHubConnectionString;isOutput=true]$TwitterDistributionEventHubConnectionString"; 
    Write-Host "##vso[task.setvariable variable=TwitterDistributionEventHubName;isOutput=true]$TwitterDistributionEventHubName";
    Write-Host "##vso[task.setvariable variable=TwitterDistributionEventHubConsumerGroup;isOutput=true]$TwitterDistributionEventHubConsumerGroup"; 
    Write-Host "##vso[task.setvariable variable=TwitterAPIKey;isOutput=true]$TwitterAPIKey";
    Write-Host "##vso[task.setvariable variable=TwitterSecretKey;isOutput=true]$TwitterSecretKey"; 
    Write-Host "##vso[task.setvariable variable=TwitterAccessToken;isOutput=true]$TwitterAccessToken";
    Write-Host "##vso[task.setvariable variable=TwitterAccessTokenSecret;isOutput=true]$TwitterAccessTokenSecret";
    Write-Host "##vso[task.setvariable variable=CognitiveServicesAccessKey;isOutput=true]$CognitiveServicesAccessKey";
    Write-Host "##vso[task.setvariable variable=CognitiveServicesEndpoint;isOutput=true]$CognitiveServicesEndpoint"; 
    Write-Host "##vso[task.setvariable variable=CVIngestionEventHubConnectionString;isOutput=true]$CVIngestionEventHubConnectionString"; 
    Write-Host "##vso[task.setvariable variable=CVIngestionEventHubName;isOutput=true]$CVIngestionEventHubName";
    Write-Host "##vso[task.setvariable variable=CVIngestionEventHubConsumerGroup;isOutput=true]$CVIngestionEventHubConsumerGroup"; 
    Write-Host "##vso[task.setvariable variable=CVDistributionEventHubConnectionString;isOutput=true]$CVDistributionEventHubConnectionString";  
    Write-Host "##vso[task.setvariable variable=CVDistributionEventHubName;isOutput=true]$CVDistributionEventHubName";  
    Write-Host "##vso[task.setvariable variable=CVDistributionEventHubConsumerGroup;isOutput=true]$CVDistributionEventHubConsumerGroup";  
    Write-Host "##vso[task.setvariable variable=ADXApplicationId;isOutput=true]$ADXApplicationId"; 
    Write-Host "##vso[task.setvariable variable=ADXApplicationKey;isOutput=true]$ADXApplicationKey"; 
    Write-Host "##vso[task.setvariable variable=ADXApplicationAuthorityId;isOutput=true]$ADXApplicationAuthorityId";    
    Write-Host "##vso[task.setvariable variable=ADXClusterName;isOutput=true]$ADXClusterName";     
    Write-Host "##vso[task.setvariable variable=CosmosDBEndPoint;isOutput=true]$CosmosDBEndPoint"; 
    Write-Host "##vso[task.setvariable variable=CosmosDBMasterKey;isOutput=true]$CosmosDBMasterKey";    
    Write-Host "##vso[task.setvariable variable=CosmosDBDatabase;isOutput=true]$CosmosDBDatabase"; 
    Write-Host "##vso[task.setvariable variable=CosmosDBPreferredRegions;isOutput=true]$CosmosDBPreferredRegions";   
    Write-Host "##vso[task.setvariable variable=CasellaStorageAccountName;isOutput=true]$CasellaStorageAccountName";     
    Write-Host "##vso[task.setvariable variable=CasellaStorageAccountKey;isOutput=true]$CasellaStorageAccountKey";  
    Write-Host "##vso[task.setvariable variable=CasellaContainerOrFileSystemName;isOutput=true]$CasellaContainerOrFileSystemName";
    Write-Host "##vso[task.setvariable variable=CasellaStorageAccountType;isOutput=true]$CasellaStorageAccountType";     
    Write-Host "##vso[task.setvariable variable=ExternalStorageAccountName;isOutput=true]$ExternalStorageAccountName";
    Write-Host "##vso[task.setvariable variable=ExternalStorageAccountKey;isOutput=true]$ExternalStorageAccountKey";
    Write-Host "##vso[task.setvariable variable=ExternalContainerOrFileSystemName;isOutput=true]$ExternalContainerOrFileSystemName";
    Write-Host "##vso[task.setvariable variable=ExternalStorageAccountType;isOutput=true]$ExternalStorageAccountType";
    Write-Host "##vso[task.setvariable variable=ExternalFileSystemUserName;isOutput=true]$ExternalFileSystemUserName";
    Write-Host "##vso[task.setvariable variable=ExternalFileSystemPwd;isOutput=true]$ExternalFileSystemPwd";
    Write-Host "##vso[task.setvariable variable=ExternalFileSystemHost;isOutput=true]$ExternalFileSystemHost";
}
