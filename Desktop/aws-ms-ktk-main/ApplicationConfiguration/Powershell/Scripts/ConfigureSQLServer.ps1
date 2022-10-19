param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DatabaseName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$Edition
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\SQL\AzureSQLModule.ps1

# Create DW Template Database
Write-Host "Creating DW Template Database" -ForegroundColor Cyan
$SQLServers = Get-AzSqlServer -ResourceGroupName $ResourceGroupName
$SQLServerName = $SQLServers[0].ServerName
New-Database -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName -DatabaseName $DatabaseName -Edition $Edition



