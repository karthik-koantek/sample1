param
(
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$SubscriptionId = "ba3707a6-d45b-4bdd-a975-41fdd1ebb123",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$ResourceGroupName = "ktk-d-mdp-e16-rg",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$ModulesDirectory = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Modules",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$StorageAccountName = "ktkdmdpe16adlssg",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$FileSystemName = "goldprotected",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$Path = "export/goldprotected.vActiveDataQualityRule/",
    [Parameter(Mandatory = $false, Position = 0)]
    [bool]$Recurse = $false,
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$LocalPath = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Tests\goldprotected.vActiveDataQualityRule.json",
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$BearerToken = "dapib42ead60fbcaa2a61c420742f88cd8f2"
)

Set-AzContext -SubscriptionId $SubscriptionId

$DatabricksWorkspace = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName
[string]$DatabricksWorkspaceUrl = $DatabricksWorkspace.Url
[string]$Machine = "https://$DatabricksWorkspaceUrl"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\ADLS\ADLSGen2Module.ps1
. $ModulesDirectory\Databricks\DatabricksClusters.ps1
. $ModulesDirectory\Databricks\DatabricksJobs.ps1

Do {
    $clustersjson = Get-DatabricksClusters -BaseURI $uribase -Token $token
    $clusters = ConvertFrom-Json $clustersjson
    $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq "SingleNode"} | Select-Object "cluster_id", "state"
    $clusterdata
    If($clusterdata.state -eq "TERMINATED") {
        Write-Host "Starting Cluster SingleNode" -ForegroundColor Cyan
        Start-DatabricksCluster -BaseURI $uribase -Token $token -ClusterName "SingleNode"
    }
    If(($clusterdata.state -eq "TERMINATED") -or ($clusterdata.state -eq "PENDING")) {
        Start-Sleep 60
    }
} While(($clusterdata.state -eq "TERMINATED") -or ($clusterdata.state -eq "PENDING"))

$ClusterId = $clusterdata.cluster_id
$NotebookPath = "/Framework/Data Quality Rules Engine/Export Data Quality Rules"
$RunName = "Export DQ Rules"

$RunInfo = Start-DatabricksNotebook -BaseURI $URIBase -Token $Token -ExistingClusterID $ClusterId -NotebookPath $NotebookPath -NotebookParams @{} -RunName $RunName
Do {
    $RunId =  ($RunInfo.Content | ConvertFrom-Json).run_id
    $RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
    Write-Host ($RunOutput.Content | ConvertFrom-Json).metadata.state
    Start-Sleep 30
} While (($RunOutput.Content | ConvertFrom-Json).metadata.state.state_message -eq "In run")
Start-Sleep 30
$RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
if(($RunOutput.Content | ConvertFrom-Json).metadata.state.result_state -ne "SUCCESS") {
    throw("Notebook Run Failed")
}
Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"

$Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
$Contents = Get-DirectoryContents -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName -FileSystemName $FileSystemName -Path $Path -Recurse $Recurse

foreach($File in $Contents){
    if($File.IsDirectory -eq $false) {
        $FilePath = $File.Path
        Get-AzDataLakeGen2ItemContent -FileSystem $FileSystemName -Path $FilePath -Context $Context -Destination $LocalPath -Force
    }
}
