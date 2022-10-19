param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$FrameworkDBAdminPwd,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$SanctionedDatabaseName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$DataFactoryName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$DataFactoryClientId,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$DataFactoryClientSecret,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$TenantId,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$FrameworkDBName,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 13)]
    [string]$TransientStorageAccountName,
    [Parameter(Mandatory = $true, Position = 14)]
    [string]$TransientStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 15)]
    [string]$BronzeStorageAccountName,
    [Parameter(Mandatory = $true, Position = 16)]
    [string]$BronzeStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 17)]
    [string]$SilverGoldStorageAccountName,
    [Parameter(Mandatory = $true, Position = 18)]
    [string]$SilverGoldStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 19)]
    [string]$SandboxStorageAccountName,
    [Parameter(Mandatory = $true, Position = 20)]
    [string]$SandboxStorageAccountKey,
    [Parameter(Mandatory = $true, Position = 21)]
    [string]$FrameworkDBFullyQualifiedServerName,
    [Parameter(Mandatory = $true, Position = 22)]
    [string]$FrameworkDBAdministratorLogin,
    [Parameter(Mandatory = $true, Position = 23)]
    [string]$AdminGroupName,
    [Parameter(Mandatory = $true, Position = 24)]
    [string]$ContributorGroupName,
    [Parameter(Mandatory = $true, Position = 25)]
    [string]$ReaderGroupName,
    [Parameter(Mandatory = $true, Position = 26)]
    [string]$BearerToken,
    [Parameter(Mandatory = $true, Position = 27)]
    [string]$WorkspaceId,
    [Parameter(Mandatory = $true, Position = 28)]
    [string]$WorkspaceUrl
)

#region begin SecretsAndScopes
#Secrets and Scopes
Write-Host "Creating internal Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName "internal"
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName "metadatadb"
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName "dwdb"
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName "adf"

Write-Host "Adding Databricks Secrets"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $BearerToken -SecretScope "internal" -SecretKey "BearerToken"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $WorkspaceId -SecretScope "internal" -SecretKey "WorkspaceId"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $WorkspaceUrl -SecretScope "internal" -SecretKey "WorkspaceUrl"

#region begin ADFSecrets
if($DataFActoryName -ne "") {
    Write-Host "Adding Secrets for Azure Data Factory"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SubscriptionId -SecretScope "adf" -SecretKey "SubscriptionId"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ResourceGroupName -SecretScope "adf" -SecretKey "ResourceGroupName"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $DataFactoryName -SecretScope "adf" -SecretKey "DataFactoryName"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $DataFactoryClientId -SecretScope "adf" -SecretKey "ClientId"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $DataFactoryClientSecret -SecretScope "adf" -SecretKey "ClientSecret"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $TenantId -SecretScope "adf" -SecretKey "TenantId"
}
#endregion
#region begin StorageAccountSecrets
Write-Host "Adding Secrets for Storage Accounts"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $TransientStorageAccountName -SecretScope "internal" -SecretKey "TransientStorageAccountName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $TransientStorageAccountKey -SecretScope "internal" -SecretKey "TransientStorageAccountKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $BronzeStorageAccountName -SecretScope "internal" -SecretKey "BronzeStorageAccountName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $BronzeStorageAccountKey -SecretScope "internal" -SecretKey "BronzeStorageAccountKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SilverGoldStorageAccountName -SecretScope "internal" -SecretKey "SilverGoldStorageAccountName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SilverGoldStorageAccountKey -SecretScope "internal" -SecretKey "SilverGoldStorageAccountKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SandboxStorageAccountName -SecretScope "internal" -SecretKey "SandboxStorageAccountName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SandboxStorageAccountKey -SecretScope "internal" -SecretKey "SandboxStorageAccountKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $DataFactoryClientId -SecretScope "internal" -SecretKey "ClientId"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $DataFactoryClientSecret -SecretScope "internal" -SecretKey "ClientSecret"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $TenantId -SecretScope "internal" -SecretKey "TenantId"

#endregion
#region begin SQLSecrets
Write-Host "Adding Secrets for Framework DB"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBFullyQualifiedServerName -SecretScope "internal" -SecretKey "FrameworkDBServerName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBName -SecretScope "internal" -SecretKey "FrameworkDBName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdministratorLogin -SecretScope "internal" -SecretKey "FrameworkDBAdministratorLogin"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdminPwd -SecretScope "internal" -SecretKey "FrameworkDBAdministratorPwd"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBFullyQualifiedServerName -SecretScope "metadatadb" -SecretKey "SQLServerName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBName -SecretScope "metadatadb" -SecretKey "DatabaseName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdministratorLogin -SecretScope "metadatadb" -SecretKey "Login"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdminPwd -SecretScope "metadatadb" -SecretKey "Pwd"
if ($SanctionedDatabaseName -ne "") {
    #Data Warehouse is optional, right now assume if it exists, it uses the same Azure SQL Server as the framework db.
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBFullyQualifiedServerName -SecretScope "internal" -SecretKey "SanctionedSQLServerName"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdministratorLogin -SecretScope "internal" -SecretKey "SanctionedSQLServerLogin"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdminPwd -SecretScope "internal" -SecretKey "SanctionedSQLServerPwd"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SanctionedDatabaseName -SecretScope "internal" -SecretKey "SanctionedDatabaseName"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBFullyQualifiedServerName -SecretScope "dwdb" -SecretKey "SQLServerName"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdministratorLogin -SecretScope "dwdb" -SecretKey "Login"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FrameworkDBAdminPwd -SecretScope "dwdb" -SecretKey "Pwd"
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SanctionedDatabaseName -SecretScope "dwdb" -SecretKey "DatabaseName"
}
#endregion
#region begin SecretACLs
Set-DatabricksSecretACLForPrincipal -BaseURI $URIBase -Token $Token -Scope "internal" -Principal $AdminGroupName -Permission "MANAGE"
Set-DatabricksSecretACLForPrincipal -BaseURI $URIBase -Token $Token -Scope "internal" -Principal $ContributorGroupName -Permission "WRITE"
Set-DatabricksSecretACLForPrincipal -BaseURI $URIBase -Token $Token -Scope "internal" -Principal $ReaderGroupName -Permission "READ"
Remove-DatabricksSecretACLForPrincipal -BaseURI $URIBase -Token $Token -Scope "internal" -Principal "users"
#endregion
#endregion
