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
    [string]$DeployJobs,
    [Parameter(Mandatory = $true, Position = 9)]
    [string]$DeployUATTesting,
    [Parameter(Mandatory = $true, Position = 10)]
    [string]$WheelLibraryPath,
    [Parameter(Mandatory = $true, Position = 11)]
    [string]$InitScriptDBFSPath,
    [Parameter(Mandatory = $true, Position = 12)]
    [hashtable]$StorageAccountHash,
    [Parameter(Mandatory = $true, Position = 13)]
    [hashtable]$StorageAccountHashUsingSecrets,
    [Parameter(Mandatory = $true, Position = 14)]
    [string]$InstancePoolId,
    [Parameter(Mandatory = $true, Position = 15)]
    [string]$DeltaEngineInstancePoolId,
    [Parameter(Mandatory = $true, Position = 16)]
    [string]$HighConcurrencyInstancePoolId,
    [Parameter(Mandatory = $true, Position = 17)]
    [string]$Environment
)

#region begin Library Coordinates
#XML
$xmlConnector = @{}
$maven = @{}
$coordinates = "com.databricks:spark-xml_2.12:0.10.0"
$maven.coordinates = $coordinates
$xmlConnector.maven = $maven
#Spark 3 SQL
$spark3SqlConnector = @{}
$maven = @{}
$coordinates = "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0"
$maven.coordinates = $coordinates
$spark3SqlConnector.maven = $maven
#Legacy SQL
$sqlConnector = @{}
$maven = @{}
$coordinates = "com.microsoft.azure:azure-sqldb-spark:1.0.2"
$maven.coordinates = $coordinates
$sqlConnector.maven = $maven
#Cosmos DB
$cosmosdb = @{}
$maven = @{}
$coordinates = "com.azure.cosmos.spark:azure-cosmos-spark_3-1_2-12:4.6.2"
$maven.coordinates = $coordinates
$cosmosdb.maven = $maven
#Great Expectations
$greatExpectations = @{}
$pypi = @{}
$package = "great_expectations"
$pypi.package = $package
$greatExpectations.pypi = $pypi
#Framework Wheel
$frameworkLibrary = @{}
$frameworkLibrary.whl = $WheelLibraryPath
#Event Hub
$eventHub = @{}
$maven = @{}
$coordinates = "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.17"
$maven.coordinates = $coordinates
$eventHub.maven = $maven
#Twitter
$twitter = @{}
$maven = @{}
$coordinates = "org.twitter4j:twitter4j-core:4.0.7"
$maven.coordinates = $coordinates
$twitter.maven = $maven
#Kusto Connector
$kustoConnector = @{}
$maven = @{}
$coordinates = "com.microsoft.azure.kusto:kusto-spark_3.0_2.12:2.9.1"
$maven.coordinates = $coordinates
$kustoConnector.maven = $maven
#Kusto Data
$kustoData = @{}
$pypi = @{}
$package = "azure-kusto-data"
$pypi.package = $package
$kustoData.pypi = $pypi
#Kusto Management
$kustoMgmt = @{}
$pypi = @{}
$package = "azure-mgmt-kusto"
$pypi.package = $package
$kustoMgmt.pypi = $pypi
#Pandas Profiling
$pandasProfiling = @{}
$pypi = @{}
$package = "pandas-profiling"
$pypi.package = $package
$pandasProfiling.pypi = $pypi
#Azure Storage File DataLake 
$azureStorageFileDataLake = @{}
$pypi = @{}
$package = "azure-storage-file-datalake"
$pypi.package = $package
$azureStorageFileDataLake.pypi = $pypi
#endregion

#region begin Multi-Task Job Settings
$SparkConf = @{}
$SparkConf."spark.scheduler.mode" = "FAIR"
if ($StorageAccountHashUsingSecrets) {
    foreach ($sa in $StorageAccountHashUsingSecrets.Keys) {
        Write-Host $sa, $StorageAccountHashUsingSecrets.$sa
        $SparkConf.$sa = $StorageAccountHashUsingSecrets.$sa
    }
}
$CustomTags = @{}
[string]$Date = Get-Date
$CustomTags.deployedDate = $Date
$SparkEnvVars = @{}
$SparkEnvVars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"
$ClusterSettingsJSON = New-ClusterSettings `
    -MinWorkers 0 `
    -MaxWorkers 1 `
    -SparkVersion $MLRuntimeSparkVersion `
    -SparkConf $SparkConf `
    -NodeType $NodeType `
    -DriverNodeType $NodeType `
    -CustomTags $CustomTags `
    -InitScript $InitScriptDBFSPath `
    -SparkEnvVars $SparkEnvVars 
$DefaultJobClusterSettings = $ClusterSettingsJSON | ConvertFrom-Json
#endregion

#region begin Jobs
if ($DeployJobs -eq "true") {
    if ($DeployUATTesting -eq "true") {
        $parameters = @{}
        $parameters.threadPool = "3"
        $parameters.timeoutSeconds = "1800"

        $libraries = @($xmlConnector, $spark3SqlConnector, $cosmosdb, $frameworkLibrary)
        $parameters.projectName = "Batch Pipeline"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Batch Ingestion Job" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets
        
        #region begin UAT_SQL
        $libraries = @($spark3SqlConnector, $frameworkLibrary, $greatExpectations)
        $CustomTags.job_name = "UAT_SQL"
        $Parameters = @{}
        $Parameters.projectName = "UAT_SQL"
        $Parameters.threadPool = 1
        $Parameters.timeoutSections = 18000
        $Parameters.whatIf = 0
        $UATNotebook = New-NotebookTaskSettings -NotebookPath "/Framework/Orchestration/Orchestration" -BaseParameters $Parameters | ConvertFrom-Json 
        $UATNotebookTask = New-TaskSettings -TaskKey "UAT" -Description "Run the UAT Pipelines" -DependsOn @() -JobFailureEmailNotification @($EmailNotification) -NewClusterSettings $DefaultJobClusterSettings -NotebookTaskSettings $UATNotebook -Libraries $Libraries | ConvertFrom-Json
        [System.Collections.ArrayList]$Tasks = @()
        $Tasks.Add($UATNotebookTask) | Out-Null 
        New-DatabricksMultiTaskJob -BaseURI $URIBase -Token $Token -JobName "UAT_SQL" -Tasks $Tasks -Format "MULTI_TASK"
        #endregion

        #region begin UAT_ExternalFiles
        $libraries = @($xmlConnector, $frameworkLibrary, $greatExpectations, $spark3SqlConnector)
        $CustomTags.job_name = "UAT_ExternalFiles_and_GoldZone"
        $Parameters = @{}
        $Parameters.projectName = "UAT_ExternalFiles_and_GoldZone"
        $Parameters.threadPool = 1
        $Parameters.timeoutSections = 18000
        $Parameters.whatIf = 0
        $UATNotebook = New-NotebookTaskSettings -NotebookPath "/Framework/Orchestration/Orchestration" -BaseParameters $Parameters | ConvertFrom-Json 
        $UATNotebookTask = New-TaskSettings -TaskKey "UAT" -Description "Run the UAT Pipelines" -DependsOn @() -JobFailureEmailNotification @($EmailNotification) -NewClusterSettings $DefaultJobClusterSettings -NotebookTaskSettings $UATNotebook -Libraries $Libraries | ConvertFrom-Json
        [System.Collections.ArrayList]$Tasks = @()
        $Tasks.Add($UATNotebookTask) | Out-Null 
        New-DatabricksMultiTaskJob -BaseURI $URIBase -Token $Token -JobName "UAT_ExternalFiles_and_GoldZone" -Tasks $Tasks -Format "MULTI_TASK"
        #endregion

        #region begin UAT_ML
        $libraries = @($spark3SqlConnector, $frameworkLibrary)
        $CustomTags.job_name = "UAT_ML"
        $Parameters = @{}
        $Parameters.projectName = "UAT_ML"
        $Parameters.threadPool = 1
        $Parameters.timeoutSections = 18000
        $Parameters.whatIf = 0
        $UATNotebook = New-NotebookTaskSettings -NotebookPath "/Framework/Orchestration/Orchestration" -BaseParameters $Parameters | ConvertFrom-Json 
        $UATNotebookTask = New-TaskSettings -TaskKey "UAT" -Description "Run the UAT Pipelines" -DependsOn @() -JobFailureEmailNotification @($EmailNotification) -NewClusterSettings $DefaultJobClusterSettings -NotebookTaskSettings $UATNotebook -Libraries $Libraries | ConvertFrom-Json
        [System.Collections.ArrayList]$Tasks = @()
        $Tasks.Add($UATNotebookTask) | Out-Null 
        New-DatabricksMultiTaskJob -BaseURI $URIBase -Token $Token -JobName "UAT_ML" -Tasks $Tasks -Format "MULTI_TASK"
        #endregion

        $libraries = @($cosmosdb, $frameworkLibrary, $spark3SqlConnector)
        $parameters.projectName = "UAT_Cosmos"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_Cosmos" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion "10.2.x-cpu-ml-scala2.12" -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets

        $libraries = @($spark3SqlConnector, $frameworkLibrary, $greatExpectations)
        $parameters.ADFProjectName = "UAT_ADF"
        $parameters.projectName = "UAT_SQL"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_ADF" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets
    
        $parameters = @{}
        $parameters.threadPool = "3"
        $parameters.timeoutSeconds = "1800"

        $libraries = @($eventHub, $twitter, $frameworkLibrary)
        $parameters.projectName = "Streaming Pipeline"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Streaming Ingestion Job" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets
        $parameters.projectName = "UAT_EventHub"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_EventHub" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets

        $libraries = @($kustoConnector, $kustoData, $kustoMgmt, $frameworkLibrary)
        $parameters = @{}
        $parameters.threadPool = "1"
        $parameters.timeoutSeconds = "1800"

        $parameters.projectName = "ADX Pipeline"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "ADX Processing Job" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHash
        $parameters.projectName = "UAT_ADX"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_ADX" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHash

        $libraries = @($pandasProfiling, $frameworkLibrary)
        $parameters.projectName = "ML Pipeline"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "ML Pipeline" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets

        #Validation
        $libraries = @($spark3SqlConnector, $greatExpectations, $frameworkLibrary)
        #$parameters.projectName = "Validation"
        #$parameters.timeoutSeconds = 18000
        #New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Validation $Environment" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -TimeoutSeconds 18000 -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MinWorkers 2 -MaxWorkers 10 -InitScript "dbfs:$DestinationPath" -InstancePoolID $DeltaEngineInstancePoolId -Libraries $libraries -Parameters $parameters -MaxRetries 0 -TimezoneID "PST" -StorageAccountHash $StorageAccountHashUsingSecrets

        $parameters.projectName = "Data Quality Assessment"
        $parameters.timeoutSeconds = 18000
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Data Quality Assessment $Environment" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -TimeoutSeconds 18000 -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MinWorkers 2 -MaxWorkers 10 -InitScript "dbfs:$DestinationPath" -InstancePoolID $DeltaEngineInstancePoolId -Libraries $libraries -Parameters $parameters -MaxRetries 0 -TimezoneID "PST" -StorageAccountHash $StorageAccountHashUsingSecrets

        $libraries = @($frameworkLibrary)
        $parameters = @{}
        $parameters.threadPool = "1"
        $parameters.timeoutSeconds = "1800"
        $parameters.projectName = "Table Maintenance"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Table Maintenance" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets
        $parameters.projectName = "UAT_Table_Maintenance"
        New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_Table_Maintenance" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MaxWorkers 2 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -StorageAccountHash $StorageAccountHashUsingSecrets
    }
    
    #Data Catalog
    $libraries = @($azureStorageFileDataLake,$spark3SqlConnector,$frameworkLibrary)
    $parameters = @{}
    $parameters.projectName = "Data Catalog"
    $parameters.timeoutSeconds = 180000
    New-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Data Catalog $Environment" -EmailNotification $EmailNotification -NotebookPath "/Framework/Orchestration/Orchestration" -TimeoutSeconds 180000 -NodeType $NodeType -SparkVersion $MLRuntimeSparkVersion -MinWorkers 2 -MaxWorkers 10 -InitScript "dbfs:$DestinationPath" -InstancePoolID $InstancePoolId -Libraries $libraries -Parameters $parameters -CronSchedule "0 0 12 ? * 2" -TimezoneID "PST" -StorageAccountHash $StorageAccountHashUsingSecrets

    # Code to create a multi-task job
    # $JobPath = $ModulesDirectory.Replace("/ApplicationConfiguration/Powershell/Modules", "/AzureDatabricks/jobs/Pipeline.json")
    # $JobJson = Get-Content -Path $JobPath -Raw
    # Write-Host "$JobJson"
    # New-DatabricksJsonJob -BaseURI $URIBase -Token $Token -JobName "Pipeline" -JobJson $JobJson
    # Write-Host "Multi Task Job Complete"
}
#endregion

#region begin Permissions 
$Users = @()
$Groups = @("Readers")
$ServicePrincipals = @()
$ReadersAccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_VIEW" | ConvertTo-Json

$Users = @()
$Groups = @("Contributors")
$ServicePrincipals = @()
$ContributorsAccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE_RUN" | ConvertTo-Json

$Jobs = ((Get-DatabricksJobs -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).jobs.job_id
foreach($JobId in $Jobs) {
    Update-JobPermissions -BaseURI $URIBase -Token $Token -JobId $JobId -AccessControlList $ReadersAccessControlList
    Update-JobPermissions -BaseURI $URIBase -Token $Token -JobId $JobId -AccessControlList $ContributorsAccessControlList
}

#endregion 