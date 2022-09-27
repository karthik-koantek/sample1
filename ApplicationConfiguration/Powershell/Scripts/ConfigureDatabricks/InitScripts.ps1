param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$SparkVersion,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$NodeType,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DeployDatabricksClusters
)

#region begin Init Script
#DBFS Uploads
#Init Script
#This works when run from powershell, but when run from cicd pipeline, the init script fails when starting cluster
#thus, running cluster init job step below to do the same thing

if ($DeployDatabricksClusters -eq "true") {
    #Cluster Init Script Job
    $InstancePoolId = Get-InstancePool -BaseURI $URIBase -Token $Token -InstancePoolName "Default"
    Write-Host "Creating Cluster Init Script Setup Job" -ForegroundColor Cyan
    New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Cluster Init Script Setup" -TimeZoneID "" -CronSchedule "" -EmailNotification "" -NotebookPath "/Framework/Orchestration/Init" -NodeType $NodeType -SparkVersion $SparkVersion -MaxWorkers 1 -InitScript "" -InstancePoolId $InstancePoolId
    Write-Host "Running Job Cluster Init Script Setup" -ForegroundColor Cyan
    Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Cluster Init Script Setup"
    Get-DatabricksJobRunsStatus -BaseURI $URIBase -Token $Token -JobName "Cluster Init Script Setup" -WaitForCompletion $true #this will wait for the job to finish before proceeding
    Write-Host "Deleting Job Cluster Init Script Setup" -ForegroundColor Cyan
    Remove-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Cluster Init Script Setup"
}
#endregion
