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
    [string]$AccessKey,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$Endpoint,
    [Parameter(Mandatory = $false, Position = 8)]
    [string]$LanguagesPath = "/text/analytics/v2.0/languages",
    [Parameter(Mandatory = $false, Position = 9)]
    [string]$SentimentPath = "/text/analytics/v2.0/sentiment"
)

Set-AzContext -SubscriptionId $SubscriptionId

[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksSecrets.ps1

Write-Host "Creating Secret Scopes and Secrets" -ForegroundColor Cyan
Add-DatabricksSecretScope -BaseURI $URIBase -Token $Token -ScopeName $ExternalSystem

Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $AccessKey -SecretScope $ExternalSystem -SecretKey "AccessKey"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $Endpoint -SecretScope $ExternalSystem -SecretKey "Endpoint"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $LanguagesPath -SecretScope $ExternalSystem -SecretKey "LanguagesPath"
Add-DatabricksSecret -BaseURI $URIBase -Token $Token -StringValue $SentimentPath -SecretScope $ExternalSystem -SecretKey "SentimentPath"
