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

Function Invoke-KustoQuery($query)
{
    [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
    $queryProvider = [Kusto.Data.Net.Client.KustoClientFactory]::CreateCslQueryProvider($kcsb)
    Write-Host "Executing query: '$query' with connection string: '$($kcsb.ToString())'"

    #   Optional: set a client request ID and set a client request property (e.g. Server Timeout)
    $crp = New-Object Kusto.Data.Common.ClientRequestProperties
    $crp.ClientRequestId = "MyPowershellScript.ExecuteQuery." + [Guid]::NewGuid().ToString()
    $crp.SetOption([Kusto.Data.Common.ClientRequestProperties]::OptionServerTimeout, [TimeSpan]::FromSeconds(30))
    $reader = $queryProvider.ExecuteQuery($query, $crp)
    $dataTable = [Kusto.Cloud.Platform.Data.ExtendedDataReader]::ToDataSet($reader).Tables[0]
    $dataView = New-Object System.Data.DataView($dataTable)

    return $dataView
}

Invoke-KustoQuery($Query)
