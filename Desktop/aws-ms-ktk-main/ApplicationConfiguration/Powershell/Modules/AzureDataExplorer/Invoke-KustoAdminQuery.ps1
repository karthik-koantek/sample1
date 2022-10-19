[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    [string]
    $KustoPackagesRoot,
    [Parameter(Mandatory=$true)]
    [string]
    $ClusterURL,
    [Parameter(Mandatory=$true)]
    [string]
    $DatabaseName,
    [Parameter(Mandatory=$true)]
    [string]
    $ServicePrincipalApplicationId,
    [Parameter(Mandatory=$true)]
    [string]
    $ServicePrincipalPassword,
    [Parameter(Mandatory=$false)]
    [string]
    $Authority = "vwcloud.onmicrosoft.com",
    [Parameter(Mandatory=$true)]
    [string]
    $Query
)

. $PSScriptRoot\KustoAuthenticate.ps1

Function Invoke-KustoAdminQuery($query)
{
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $adminProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslAdminProvider($kcsb)
    Write-Host "Executing command: '$query' with connection string: '$($kcsb.ToString())'"
    $reader = $adminProvider.ExecuteControlCommand($query)
    return $reader
}

Invoke-KustoAdminQuery($Query)
