Set-AzContext -SubscriptionId "cb6f8c17-5a8b-41cb-9e84-e3bd57a193de"
#Install-Module -Name Az.Kusto -RequiredVersion 0.2.0
#Install-Module -Name Az.Accounts -RequiredVersion 1.8.1
Import-Module Az.Kusto
$KustoCluster = Get-AzKustoCluster -ResourceGroupName "DatabricksVirtualDataConnectorSources"
$KustoCluster | Start-AzKustoCluster
$KustoCluster | Stop-AzKustoCluster

#UAT Testing Development (DVCatalyst Local)
[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$Region = "westus2"
[string]$BearerToken = "dapid31374cad0f020a0457a4f22bd515c9e"
[string]$PowershellDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Tests\RunUATJobs.ps1' `
-SubscriptionId $SubscriptionId `
-Region $Region `
-BearerToken $BearerToken `
-PowershellDirectory $PowershellDirectory

#UAT Testing Production (DVCatalyst Local)
[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$Region = "westus2"
[string]$BearerToken = "dapi4efe7360ae7b32eb3d92b5059be65144"
[string]$PowershellDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Tests\RunUATJobs.ps1' `
-SubscriptionId $SubscriptionId `
-Region $Region `
-BearerToken $BearerToken `
-PowershellDirectory $PowershellDirectory

#UAT Testing Development (Digital Insights)
[string]$SubscriptionId = "ba3707a6-d45b-4bdd-a975-41fdd1ebb123"
[string]$Region = "westus2"
[string]$BearerToken = "dapi687e4acf7577e575f91bb42e1ed66d09"
[string]$PowershellDirectory = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell"
& 'C:\src\DITeamDatabricks\DataValueCatalyst\ApplicationConfiguration\Powershell\Tests\RunUATJobs.ps1' `
-SubscriptionId $SubscriptionId `
-Region $Region `
-BearerToken $BearerToken `
-PowershellDirectory $PowershellDirectory

#UAT Testing Production (Digital Insights)
[string]$SubscriptionId = "ba3707a6-d45b-4bdd-a975-41fdd1ebb123"
[string]$Region = "westus2"
[string]$BearerToken = "dapi2d60a8678555ce9a6fd1bb5747350888"
[string]$PowershellDirectory = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell"
& 'C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Tests\RunUATJobs.ps1' `
-SubscriptionId $SubscriptionId `
-Region $Region `
-BearerToken $BearerToken `
-PowershellDirectory $PowershellDirectory