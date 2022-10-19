param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$BaseString,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$Salt,
    [Parameter(Mandatory = $false, Position = 2)]
    [int]$Length = 3,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$Environment,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$Project,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$Service
)

. $ModulesDirectory\Naming\NamingModule.ps1

[string]$UniqueString = New-UniqueString -String $BaseString -Salt $Salt -Length $Length
[string]$EnvironmentAbbreviation = $Environment[0]
[string]$NamingPattern = "$Project-$EnvironmentAbbreviation-$Service-$UniqueString"
[string]$ResourceGroupName = "$NamingPattern-rg"
[string]$DatabricksWorkspaceName = "$NamingPattern-dbws"
[string]$TransientStorageAccountName = "$NamingPattern-blob-t".replace("-","")
[string]$BronzeStorageAccountName = "$NamingPattern-adls-b".replace("-","")
[string]$SilverGoldStorageAccountName = "$NamingPattern-adls-sg".replace("-","")
[string]$SandboxStorageAccountName = "$NamingPattern-adls-sb".replace("-","")
[string]$AzureSQLServerName = "$NamingPattern-sql"
[string]$FullyQualifiedAzureSQLServerName = "$AzureSQLServerName.database.windows.net"
[string]$AzureSQLDatabaseName = "$NamingPattern"
[string]$KeyVaultName = "$NamingPattern-kv"
[string]$AzureDataFactoryName = "$NamingPattern-adf"

Write-Host "Resource Group Name: $ResourceGroupName"
Write-Host "Databricks Workspace Name: $DatabricksWorkspaceName"
Write-Host "Transient Storage AccountName: $TransientStorageAccountName"
Write-Host "Bronze Storage Account Name: $BronzeStorageAccountName"
Write-Host "Silver/Gold Storage Account Name: $SilverGoldStorageAccountName"
Write-Host "Sandbox Storage Account Name: $SandboxStorageAccountName"
Write-Host "Azure SQL Server Name: $AzureSQLServerName"
Write-Host "Fully Qualified Azure SQL Server Name:" $FullyQualifiedAzureSQLServerName
Write-Host "Azure SQL Database Name: $AzureSQLDatabaseName"
Write-Host "Key Vault Name: $KeyVaultName"
Write-Host "Azure Data Factory Name: $AzureDataFactoryName"

Write-Host "##vso[task.setvariable variable=RESOURCE_GROUP_NAME;isOutput=true]$ResourceGroupName";
Write-Host "##vso[task.setvariable variable=DATABRICKS_WORKSPACE_NAME;isOutput=true]$DatabricksWorkspaceName";
Write-Host "##vso[task.setvariable variable=TRANSIENT_STORAGE_ACCOUNT_NAME;isOutput=true]$TransientStorageAccountName";
Write-Host "##vso[task.setvariable variable=BRONZE_STORAGE_ACCOUNT_NAME;isOutput=true]$BronzeStorageAccountName";
Write-Host "##vso[task.setvariable variable=SILVERGOLD_STORAGE_ACCOUNT_NAME;isOutput=true]$SilverGoldStorageAccountName";
Write-Host "##vso[task.setvariable variable=SANDBOX_STORAGE_ACCOUNT_NAME;isOutput=true]$SandboxStorageAccountName";
Write-Host "##vso[task.setvariable variable=ACCELERATOR_DB_SERVER_NAME;isOutput=true]$AzureSQLServerName";
Write-Host "##vso[task.setvariable variable=FULLY_QUALIFIED_ACCELERATOR_DB_SERVER_NAME;isOutput=true]$FullyQualifiedAzureSQLServerName";
Write-Host "##vso[task.setvariable variable=DW_DATABASE_NAME;isOutput=true]$AzureSQLDatabaseName";
Write-Host "##vso[task.setvariable variable=KEY_VAULT_NAME;isOutput=true]$KeyVaultName";
Write-Host "##vso[task.setvariable variable=DATA_FACTORY_NAME;isOutput=true]$AzureDataFactoryName";