param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$SparkVersion,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$MLRuntimeSparkVersion,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$NodeType,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$DeltaEngineNodeType,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$EmailNotification,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$DeployDatabricksClusters,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$InitScriptDBFSPath,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$InstancePoolId,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$DeltaEngineInstancePoolId,
    [Parameter(Mandatory = $true, Position = 12)]
    [string]$HighConcurrencyInstancePoolId,
    [Parameter(Mandatory = $true, Position = 13)]
    [hashtable]$StorageAccountHash,
    [Parameter(Mandatory = $true, Position = 14)]
    [hashtable]$StorageAccountHashUsingSecrets,
    [Parameter(Mandatory = $true, Position = 15)]
    [string]$WheelLibraryPath
)

#region begin Clusters

if ($DeployDatabricksClusters -eq "true") {
    #region begin CreateClusters
    #Default Clusters
    Write-Host "Creating Default Cluster" -ForegroundColor Cyan
    New-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Default" -MinWorkers 1 -MaxWorkers 2 -AutoTerminationMinutes 10 -SparkVersion $MLRuntimeSparkVersion -NodeType $NodeType -DriverNodeType $NodeType -InitScriptPath "dbfs:$InitScriptDBFSPath" -InstancePoolId $InstancePoolId -StorageAccountHash $StorageAccountHashUsingSecrets
    Write-Host "Creating HighConcurrency Cluster" -ForegroundColor Cyan
    New-DatabricksHighConcurrencyCluster -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency" -MinWorkers 1 -MaxWorkers 2 -AutoTerminationMinutes 10 -SparkVersion $SparkVersion -NodeType $NodeType -DriverNodeType $NodeType -InstancePoolID $HighConcurrencyInstancePoolId -StorageAccountHash $StorageAccountHashUsingSecrets -EnableTableAccessControl $true -EnableADLSCredentialPassthrough $false
    Write-Host "Creating Power BI Cluster" -ForegroundColor Cyan
    New-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Power BI" -MinWorkers 1 -MaxWorkers 2 -AutoTerminationMinutes 10 -SparkVersion $MLRuntimeSparkVersion -NodeType $NodeType -DriverNodeType $NodeType -InstancePoolID $DeltaEngineInstancePoolId -StorageAccountHash $StorageAccountHashUsingSecrets
    Write-Host "Creating Single Node Cluster" -ForegroundColor Cyan
    New-DatabricksSingleNodeCluster -BaseURI $URIBase -Token $Token -ClusterName "SingleNode" -AutoTerminationMinutes 10 -SparkVersion $MLRuntimeSparkVersion -NodeType $NodeType -InitScriptPath "dbfs:$InitScriptDBFSPath" -InstancePoolId $InstancePoolId -StorageAccountHash $StorageAccountHashUsingSecrets

    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Default"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Power BI"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"
    Start-Sleep 60

    #endregion

    #region begin Permissions
    $Users = @()
    $Groups = @("Readers")
    $ServicePrincipals = @()
    $ReadersAccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_ATTACH_TO" | ConvertTo-Json

    $Users = @()
    $Groups = @("Contributors")
    $ServicePrincipals = @()
    $ContributorsAccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_RESTART" | ConvertTo-Json

    $DefaultClusterId = Get-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Default"
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $DefaultClusterId -AccessControlList $ReadersAccessControlList
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $DefaultClusterId -AccessControlList $ContributorsAccessControlList

    $SingleNodeClusterId = Get-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $SingleNodeClusterId -AccessControlList $ReadersAccessControlList
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $SingleNodeClusterId -AccessControlList $ContributorsAccessControlList

    $PowerBIClusterId = Get-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Power BI"
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $PowerBIClusterId -AccessControlList $ReadersAccessControlList
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $PowerBIClusterId -AccessControlList $ContributorsAccessControlList
  
    $HighConcurrencyClusterId = Get-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency"
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $HighConcurrencyClusterId -AccessControlList $ReadersAccessControlList
    Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId $HighConcurrencyClusterId -AccessControlList $ContributorsAccessControlList

    #endregion 

    #region begin cluster libraries
    #Default
    Start-DatabricksClusterAwaitUntilStarted -BaseURI $URIBase -Token $Token -ClusterName "Default"
    Write-Host "Installing Databricks Framework Python Library" -ForegroundColor Cyan
    New-ClusterCustomLibrary -BaseURI $URIBase -Token $Token -Path $WheelLibraryPath -ClusterName "Default" -LibraryType "whl"
    New-ClusterMavenLibrary -BaseURI $URIBase -Token $Token -ClusterName "Default" -MavenCoordinate "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0"
    New-ClusterMavenLibrary -BaseURI $URIBase -Token $Token -ClusterName "Default" -MavenCoordinate "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.3.0"
    New-ClusterMavenLibrary -BaseURI $URIBase -Token $Token -ClusterName "Default" -MavenCoordinate "com.databricks:spark-xml_2.12:0.12.0"
    New-ClusterPyPiLibrary -BaseURI $URIBase -Token $Token -ClusterName "Default" -PyPiPackage "great_expectations"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Default"
    #HighConcurrency
    $clusterdata = Start-DatabricksClusterAwaitUntilStarted -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency"
    Write-Host "Installing Databricks Framework Python Library" -ForegroundColor Cyan
    New-ClusterCustomLibrary -BaseURI $URIBase -Token $Token -Path $WheelLibraryPath -ClusterName "HighConcurrency" -LibraryType "whl"
    Start-Sleep 60
    Write-Host "Running Data Object Privileges Notebook" -ForegroundColor Cyan
    $ClusterId = $clusterdata.cluster_id[0]
    $NotebookPath = "/Framework/Orchestration/Table Access Control Data Object Privileges"
    $RunName = "Data Object Privileges"
    $RunInfo = Start-DatabricksNotebook -BaseURI $URIBase -Token $Token -ExistingClusterID $ClusterId -NotebookPath $NotebookPath -NotebookParams @{} -RunName $RunName
    Do {
        $RunId =  ($RunInfo.Content | ConvertFrom-Json).run_id
        $RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
        Write-Host ($RunOutput.Content | ConvertFrom-Json).metadata.state
        Start-Sleep 60
    } While (($RunOutput.Content | ConvertFrom-Json).metadata.state.state_message -eq "In run")
    Start-Sleep 30
    $RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
    if(($RunOutput.Content | ConvertFrom-Json).metadata.state.result_state -ne "SUCCESS") {
        throw("Notebook Run $RunName Failed")
    }
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "HighConcurrency"
    #Power BI
    Start-DatabricksClusterAwaitUntilStarted -BaseURI $URIBase -Token $Token -ClusterName "Power BI"
    Write-Host "Installing Databricks Framework Python Library" -ForegroundColor Cyan
    New-ClusterCustomLibrary -BaseURI $URIBase -Token $Token -Path $WheelLibraryPath -ClusterName "Power BI" -LibraryType "whl"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "Power BI"
    #SingleNode
    Start-DatabricksClusterAwaitUntilStarted -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"
    Write-Host "Installing Databricks Framework Python Library" -ForegroundColor Cyan
    New-ClusterCustomLibrary -BaseURI $URIBase -Token $Token -Path $WheelLibraryPath -ClusterName "SingleNode" -LibraryType "whl"
    Stop-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"
    #endregion
}
#endregion
