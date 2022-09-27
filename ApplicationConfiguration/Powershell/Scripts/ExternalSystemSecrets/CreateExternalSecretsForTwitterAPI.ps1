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
    [string]$APIKey,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$SecretKey,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$AccessToken,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$AccessTokenSecret
)

#The secrets created by this script are intended to align with the Twitter Developer API: https://developer.twitter.com/en

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

Write-Host "Creating Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName $ExternalSystem

Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $APIKey -SecretScope $ExternalSystem -SecretKey "APIKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SecretKey -SecretScope $ExternalSystem -SecretKey "SecretKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $AccessToken -SecretScope $ExternalSystem -SecretKey "AccessToken"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $AccessTokenSecret -SecretScope $ExternalSystem -SecretKey "AccessTokenSecret"
