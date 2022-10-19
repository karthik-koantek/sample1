dir $KustoPackagesRoot\* | Unblock-File
[System.Reflection.Assembly]::LoadFrom("$KustoPackagesRoot\Kusto.Data.dll")

$cluster = "$ClusterURL;Fed=True"

New-Object Kusto.Data.KustoConnectionStringBuilder

$kcsb = New-Object ('Kusto.Data.KustoConnectionStringBuilder') -ArgumentList @($cluster, $DatabaseName)
$kcsb = $kcsb.WithAadApplicationKeyAuthentication($ServicePrincipalApplicationId, $ServicePrincipalPassword, $Authority)
