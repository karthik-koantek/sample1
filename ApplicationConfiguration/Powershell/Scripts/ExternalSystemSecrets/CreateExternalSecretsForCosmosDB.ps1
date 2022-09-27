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
    [string]$EndPoint,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$MasterKey,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$Database,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$PreferredRegions
)

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

Write-Host "Creating Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName $ExternalSystem

Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $EndPoint -SecretScope $ExternalSystem -SecretKey "EndPoint"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $MasterKey -SecretScope $ExternalSystem -SecretKey "MasterKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $Database -SecretScope $ExternalSystem -SecretKey "Database"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $PreferredRegions -SecretScope $ExternalSystem -SecretKey "PreferredRegions"
