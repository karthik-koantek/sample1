
New-DeltaLiveTablePipeline `
-BaseURI $URIBase `
-Token $Token `
-PipelineName "DLT CloudFiles" `
-Storage "/Users/eedgeworth@databricks.com/data" `
-PipelineNotebooks @("/Users/eedgeworth@valorem.com/DLT/DLT CloudFiles") `
-Target "silvergeneral" `
-Continuous $false `
-DevelopmentMode $true `
-NodeType $NodeType `
-DriverNodeType $NodeType `
-MinWorkers 0 `
-MaxWorkers 1 `
-AutoTerminationMinutes 10 `
-InitScriptPath "dbfs:$DestinationPath" `
-StorageAccountHash $StorageAccountHashUsingSecrets `
-CreateMaintenanceCluster $true `
-ConfigurationHash @{}

$ConfigurationHash = @{
"externalSystem"="internal"
"fileExtension"="csv"
"header"="False"
"adding1"="1"
}
New-DeltaLiveTablePipeline `
-BaseURI $URIBase `
-Token $Token `
-PipelineName "DLT Samples" `
-Storage "/Users/eedgeworth@databricks.com/data" `
-PipelineNotebooks @("/Users/eedgeworth@valorem.com/DLT/Delta Live Tables quickstart (Python)", "/Users/eedgeworth@valorem.com/DLT/Delta Live Tables quickstart (SQL)") `
-Target "silvergeneral" `
-Continuous $false `
-DevelopmentMode $true `
-NodeType $NodeType `
-DriverNodeType $NodeType `
-MinWorkers 0 `
-MaxWorkers 1 `
-AutoTerminationMinutes 10 `
-InitScriptPath "dbfs:$DestinationPath" `
-StorageAccountHash $StorageAccountHashUsingSecrets `
-CreateMaintenanceCluster $true `
-ConfigurationHash $ConfigurationHash

$response = Get-DeltaLiveTablePipelines `
-BaseURI $URIBase `
-Token $Token `
-MaxResults 1 `
-Filter "name LIKE 'DLT Samples'"

$response = Get-DeltaLiveTablePipelines `
-BaseURI $URIBase `
-Token $Token `
-MaxResults 1 `
-PageToken ($response.Content | ConvertFrom-Json).next_page_token

$response = Remove-DeltaLiveTablePipeline -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples"

$response = Get-DeltaLiveTablePipelineDetails -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples"

$response = Update-DeltaLiveTablePipeline -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples" -Continuous $false -DevelopmentMode $false -PipelineNotebooks @("/Users/eedgeworth@valorem.com/DLT/Delta Live Tables quickstart (Python)", "/Users/eedgeworth@valorem.com/DLT/Delta Live Tables quickstart (SQL)")

$response = Start-DeltaLiveTablePipeline -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples"
$update_id = ($response.Content | ConvertFrom-Json).update_id
$response = Get-DeltaLiveTablePipelineUpdateDetails -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples" -UpdateId $update_id

$response = Get-DeltaLiveTablePipelineEvents -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples"
$response = Get-DeltaLiveTablePipelineEvents -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples" -Filter "level='ERROR'"

$response = Get-DeltaLiveTablePipelineUpdateDetails -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples" -UpdateId $update_id

$response = Stop-DeltaLiveTablePipeline -BaseURI $URIBase -Token $Token -PipelineName $PipelineName

$SparkConf = @{}
$SparkConf."spark.scheduler.mode" = "FAIR"
if ($StorageAccountHashUsingSecrets) {
    foreach ($sa in $StorageAccountHashUsingSecrets.Keys) {
        Write-Host $sa, $StorageAccountHashUsingSecrets.$sa
        $SparkConf.$sa = $StorageAccountHashUsingSecrets.$sa
    }
}
$CustomTags = @{}
$CustomTags.job_name = "Multi-task job test"
$CustomTags.author = "Eddie Edgeworth"
[string]$Date = Get-Date
$CustomTags.deployedDate = $Date
$SparkEnvVars = @{}
$SparkEnvVars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"
$DLTSamplesPipelineId = ((Get-DeltaLiveTablePipelineDetails -BaseURI $URIBase -Token $Token -PipelineName "DLT Samples").Content | ConvertFrom-Json).pipeline_id

$ClusterSettingsJSON = New-ClusterSettings `
    -MinWorkers 0 `
    -MaxWorkers 1 `
    -SparkVersion "8.4.x-scala2.12" `
    -SparkConf $SparkConf `
    -NodeType "Standard_DS3_v2" `
    -DriverNodeType "Standard_DS5_v2" `
    -CustomTags $CustomTags `
    -InitScript "/databricks/pyodbc.sh" `
    -SparkEnvVars $SparkEnvVars 
$ClusterSettings = $ClusterSettingsJSON | ConvertFrom-Json
$DLTSamplesPipelineJSON = New-PipelineTaskSettings `
    -PipelineId $DLTSamplesPipelineId 
$DLTSamplesPipeline = $DLTSamplesPipelineJSON | ConvertFrom-Json 
$DLTSamplesPipelineTaskJSON = New-TaskSettings `
    -TaskKey "DLTSamples" `
    -Description "Run DLT Samples Pipeline" `
    -DependsOn @() `
    -JobFailureEmailNotification @("eedgeworth@valorem.com") `
    -PipelineTaskSettings $DLTSamplesPipeline 
$DLTSamplesPipelineTask = $DLTSamplesPipelineTaskJSON | ConvertFrom-Json 
$Parameters = @{}
$Parameters.timeoutSeconds = 18000
$Parameters.threadPool = 1
$Parameters.projectName = "Data Catalog"
$DataCatalogNotebookJSON = New-NotebookTaskSettings `
    -NotebookPath "/Framework/Orchestration/Orchestration" `
    -BaseParameters $Parameters
$DataCatalogNotebook = $DataCatalogNotebookJSON | ConvertFrom-Json 
$DataCatalogTaskJSON = New-TaskSettings `
    -TaskKey "DataCatalog" `
    -Description "Run Data Catalog" `
    -DependsOn @("DLTSamples") `
    -JobFailureEmailNotification @("eedgeworth@valorem.com") `
    -NewClusterSettings $ClusterSettings `
    -NotebookTaskSettings $DataCatalogNotebook
$DataCatalogTask = $DataCatalogTaskJSON | ConvertFrom-Json
[System.Collections.ArrayList]$Tasks = @()
$Tasks.Add($DLTSamplesPipelineTask) | Out-Null
$Tasks.Add($DataCatalogTask) | Out-Null
$Response = New-DatabricksMultiTaskJob `
    -BaseURI $URIBase `
    -Token $Token `
    -JobName "Eddie Test Multitask Job" `
    -Tasks $Tasks `
    -Format "MULTI_TASK"


$JSONFilePath = "C:\src\DITeamDatabricks\DataValueCatalyst\AzureDatabricks\jobs\EddieTest.json"
$JobJson = Get-Content -Raw -Path $JSONFilePath
$response = New-DatabricksJsonJob `
    -BaseURI $URIBase `
    -Token $Token `
    -JobName "EddieTesting" `
    -JobJson $JobJson

#$TenantId="5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd"
#$ClientId="b674c80e-baf9-4aac-ac2c-5e3fb13aa35a"
#$ClientSecret="xAVAzCc~D2BD4OhNqh7i-v68N5~~pwz_.5"
##$ClientSecretSecure = ConvertTo-SecureString -String $ClientSecret -AsPlainText -Force
#$AADBearerToken = Get-AADBearerToken -TenantId $TenantId -ClientId $ClientId -ClientSecret $ClientSecret
#
#$DevOpsToken = "ghk3xm45olazw5vbuexa5r4mcvcgoxyjo66lgigldggh3uole6ka"
#$DevOpsTokenSecure = ConvertTo-SecureString -String $DevOpsToken -AsPlainText -Force

New-DatabricksRepo -BaseURI $URIBase -Token $ADBTokenSecure -URL "https://vcg.visualstudio.com/DataBricks%20Framework/_git/DataValueCatalyst" -Provider "azureDevOpsServices" -Path "/Repos/Production/DataValueCatalyst"
#New-DatabricksRepo -BaseURI $URIBase -Token $DevOpsToken -URL "https://vcg.visualstudio.com/DataBricks%20Framework/_git/DataValueCatalyst" -Provider "azureDevOpsServices" -Path "/Repos/eedgeworth@valorem.com/DataValueCatalyst3"
#New-DatabricksRepo -BaseURI $URIBase -Token $AADBearerToken -URL "https://vcg.visualstudio.com/DataBricks%20Framework/_git/DataValueCatalyst" -Provider "azureDevOpsServices" -Path "/Repos/eedgeworth@valorem.com/DataValueCatalyst3"

#Token must represent a user with permissions to both ADO and Databricks.  Here I'm using a personally generated token:
$ADBToken = "dapibd7b138dd4cf5205f2b048e36545a7cf"
$ADBTokenSecure = ConvertTo-SecureString -String $ADBToken -AsPlainText -Force

New-DatabricksRepo -BaseURI $URIBase -Token $ADBTokenSecure -URL "https://vcg.visualstudio.com/DataBricks%20Framework/_git/DataValueCatalyst" -Provider "azureDevOpsServices" -Path "/Repos/Production/DataValueCatalyst"
New-DatabricksRepo -BaseURI $URIBase -Token $ADBTokenSecure -URL "https://vcg.visualstudio.com/DataBricks%20Framework/_git/DataValueCatalyst" -Provider "azureDevOpsServices" -Path "/Repos/Development/DataValueCatalyst"
$Response = Update-DatabricksRepo -BaseURI $URIBase -Token $ADBTokenSecure -RepoId $DevRepo.Id -Branch "features/dvcaugust"
$Response = Update-DatabricksRepo -BaseURI $URIBase -Token $ADBTokenSecure -RepoId $ProductionRepo.Id -Branch "master"

((Get-DatabricksRepos -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).repos

$ProductionRepo = ((Get-DatabricksRepos -BaseURI $URIBase -Token $Token -PathPrefix "/Repos/Production/DataValueCatalyst").Content | ConvertFrom-Json).repos
$DevRepo = ((Get-DatabricksRepos -BaseURI $URIBase -Token $Token -PathPrefix "/Repos/Development/DataValueCatalyst").Content | ConvertFrom-Json).repos

$Response = Remove-DatabricksRepo -BaseURI $URIBase -Token $Token -RepoId $ProductionRepo.id
Get-DatabricksRepo -BaseURI $URIBase -Token $Token -RepoId "479937892838734"