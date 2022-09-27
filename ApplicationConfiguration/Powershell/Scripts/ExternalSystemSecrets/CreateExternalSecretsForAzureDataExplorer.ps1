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
    [string]$ADXApplicationId,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$ADXApplicationKey,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$ADXApplicationAuthorityId,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$ADXClusterName
)

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

Write-Host "Creating Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName $ExternalSystem

Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ADXApplicationId -SecretScope $ExternalSystem -SecretKey "ADXApplicationId"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ADXApplicationKey -SecretScope $ExternalSystem -SecretKey "ADXApplicationKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ADXApplicationAuthorityId -SecretScope $ExternalSystem -SecretKey "ADXApplicationAuthorityId"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $ADXClusterName -SecretScope $ExternalSystem -SecretKey "ADXClusterName"
