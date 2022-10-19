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
    [string]$ExternalSystem,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$StorageAccountName,
    [Parameter(Mandatory = $false, Position = 7)]
    [string]$StorageAccountKey = "",
    [Parameter(Mandatory = $false, Position = 8)]
    [string]$SharedAccessSignature = "",
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$ContainerOrFileSystemName,
    [Parameter(Mandatory = $false, Position = 10)]
    [string]$StorageAccountType = "blob", #values: adlsv2, blob
    [Parameter(Mandatory = $false, Position = 11)]
    [string]$ClientId = "",
    [Parameter(Mandatory = $false, Position = 12)]
    [string]$ClientSecret = "",
    [Parameter(Mandatory = $false, Position = 13)]
    [string]$TenantId = ""
)

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

Write-Host "Creating Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName $ExternalSystem

if ($StorageAccountType -eq "blob") {
    if ($SharedAccessSignature -ne "") {
        $FullStorageAccountName = "fs.azure.sas.$ContainerOrFileSystemName.$StorageAccountName.blob.core.windows.net"
    } else {
        $FullStorageAccountName = "fs.azure.account.key.$StorageAccountName.blob.core.windows.net"
    }
    $BasePath = "wasbs://$ContainerOrFileSystemName@$StorageAccountName.blob.core.windows.net"
} else {
    if ($StorageAccountType -eq "adlsv2") {
        if ($SharedAccessSignature -ne "") {
            $FullStorageAccountName = "fs.azure.sas.$ContainerOrFileSystemName.$StorageAccountName.dfs.core.windows.net"
        } else {
            $FullStorageAccountName = "fs.azure.account.key.$StorageAccountName.dfs.core.windows.net"
        }
        $BasePath = "abfss://$ContainerOrFileSystemName@$StorageAccountName.dfs.core.windows.net"
    } else {
        Throw 'Invalid Storage Account Type.'
    }
}

Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $FullStorageAccountName -SecretScope $ExternalSystem -SecretKey "StorageAccountName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ContainerOrFileSystemName -SecretScope $ExternalSystem -SecretKey "ContainerOrFileSystemName"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $BasePath -SecretScope $ExternalSystem -SecretKey "BasePath"

if ($StorageAccountKey -ne "") {
    Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $StorageAccountKey -SecretScope $ExternalSystem -SecretKey "StorageAccountKey"
} else {
    if ($SharedAccessSignature -ne "") {
        Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SharedAccessSignature -SecretScope $ExternalSystem -SecretKey "StorageAccountKey"
    } else {
        if ($ClientId -ne "" -and $ClientSecret -ne "" -and $TenantId -ne "") {
            Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ClientId -SecretScope $ExternalSystem -SecretKey "ClientId"
            Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ClientSecret -SecretScope $ExternalSystem -SecretKey "ClientSecret"
            Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $TenantId -SecretScope $ExternalSystem -SecretKey "TenantId"
        } else {
            Throw 'Either Storage Account Key, SAS, or Service Principal secrets must be supplied.'
        }
    }
}
