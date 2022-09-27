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
    [string]$Environment
)

. $ModulesDirectory\Naming\NamingModule.ps1

[string]$UniqueString = New-UniqueString -String $BaseString -Salt $Salt -Length $Length
[string]$ResourceGroupName = "$BaseString$Environment$UniqueString"
[string]$DatabricksWorkspaceName = "$BaseString$Environment$UniqueString"
[string]$StorageAccountName = "$BaseString$Environment$UniqueString"
[string]$AzureSQLServerName = "$BaseString$Environment$UniqueString.database.windows.net"
[string]$AzureSQLDatabaseName = "$BaseString$Environment$UniqueString"

Write-Host "Resource Group Name: $ResourceGroupName"
Write-Host "Databricks Workspace Name: $DatabricksWorkspaceName"
Write-Host "Storage Account Name: $StorageAccountName"
Write-Host "Azure SQL Server Name: $AzureSQLServerName"
Write-Host "Azure SQL Database Name: $AzureSQLDatabaseName"

Write-Host "##vso[task.setvariable variable=RESOURCE_GROUP_NAME;isOutput=true]$ResourceGroupName";
Write-Host "##vso[task.setvariable variable=DATABRICKS_WORKSPACE_NAME;isOutput=true]$DatabricksWorkspaceName";
Write-Host "##vso[task.setvariable variable=ADLS_STORAGE_ACCOUNT_NAME;isOutput=true]$StorageAccountName";
Write-Host "##vso[task.setvariable variable=ACCELERATOR_DB_SERVER_NAME;isOutput=true]$AzureSQLServerName";
Write-Host "##vso[task.setvariable variable=DW_DATABASE_NAME;isOutput=true]$AzureSQLDatabaseName";