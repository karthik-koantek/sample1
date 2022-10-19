. $PSScriptRoot\Helper.ps1

Function Get-DeltaLiveTablePipelines {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$false, Position = 2)]
        [string]$PageToken = "",
        [Parameter(Mandatory=$false, Position = 3)]
        [string]$MaxResults = 100,
        [Parameter(Mandatory=$false, Position= 4)]
        [string]$OrderBy = "name asc",
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$Filter = ""
    )

    $PipelineRequest = @{}
    if($PageToken -ne "") {$PipelineRequest.page_token = $PageToken}
    $PipelineRequest.max_results = $MaxResults 
    if($OrderBy -ne "") {$PipelineRequest.order_by = $OrderBy} 
    if($Filter -ne "") {$PipelineRequest.filter = $Filter}
    $PipelineRequestJSON = $PipelineRequest | ConvertTo-Json

    Return Invoke-GetRESTRequest -URI "${BaseURI}/pipelines" -Token $Token -Body $PipelineRequestJSON
}

Function New-DeltaLiveTablePipeline {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$Storage,
        [Parameter(Mandatory=$true, Position = 4)]
        [System.Collections.ArrayList]$PipelineNotebooks,
        [Parameter(Mandatory=$true, Position = 5)]
        [string]$Target,
        [Parameter(Mandatory=$true, Position = 6)]
        [bool]$Continuous,
        [Parameter(Mandatory=$false, Position = 7)]
        [bool]$DevelopmentMode = $false,
        [Parameter(Mandatory=$true, Position = 7)]
		[string]$NodeType,
		[Parameter(Mandatory=$true, Position = 8)]
		[string]$DriverNodeType,
        [Parameter(Mandatory=$false, Position = 9)]
		[int]$MinWorkers = 0,
        [Parameter(Mandatory=$true, Position = 10)]
		[int]$MaxWorkers,
		[Parameter(Mandatory=$true, Position = 11)]
		[int]$AutoTerminationMinutes,
        [Parameter(Mandatory=$false, Position = 12)]
		[string]$InitScriptPath = "",
		[Parameter(Mandatory=$false, Position = 13)]
		$StorageAccountHash = @{},
        [Parameter(Mandatory=$false, Position = 14)]
        [bool]$CreateMaintenanceCluster = $false,
        [Parameter(Mandatory=$false, Position = 15)]
        $ConfigurationHash = @{}
    )

    $exists = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.name
    if(!$exists) {
        $PipelineSettings = @{}
        $PipelineSettings.name = $PipelineName
        $PipelineSettings.storage = $Storage 
        $PipelineSettings.target = $Target 
        $PipelineSettings.continuous = $Continuous
        $PipelineSettings.development = $DevelopmentMode
        [System.Collections.ArrayList]$libraries = @()
        foreach($PipelineNotebook in $PipelineNotebooks) {
            $library = @{}
            $notebook = @{}
            $notebook.path = $PipelineNotebook
            $library.notebook = $notebook
            $libraries.add($library)
        }
        $PipelineSettings.libraries = $libraries
    
        #Configuration
        $configuration = @{}
        if ($ConfigurationHash) {
            foreach ($Config in $ConfigurationHash.Keys) {
                #Write-Host $Config, $ConfigurationHash.$Config
                $Configuration.$Config = $ConfigurationHash.$Config
            }
        }
        $PipelineSettings.configuration = $configuration
    
        #region begin Clusters
        #region begin Default Cluster
        #default cluster
        $DefaultCluster = @{}
        $DefaultCluster.label = "default"
        $DefaultCluster.node_type_id = $NodeType 
        $DefaultCluster.driver_node_type_id = $DriverNodeType
        $DefaultCluster.autotermination_minutes = $AutoTerminationMinutes
    
        #workers
        if($MinWorkers -eq 0) {
            $DefaultCluster.num_workers = $MaxWorkers
        } else {
            $autoscaleinfo = @{}
            $autoscaleinfo.min_workers = $MinWorkers
            $autoscaleinfo.max_workers = $MaxWorkers
            $autoscale = New-Object -TypeName PSObject -Property $autoscaleinfo
            $DefaultCluster.autoscale = $autoscale
        }
    
        #env_vars
        $spark_env_vars = @{}
        $spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"
        $DefaultCluster.spark_env_vars = $spark_env_vars
    
        #spark.conf
        $spark_conf = @{}
        $spark_conf."spark.scheduler.mode" = "FAIR"
        $spark_conf."spark.sql.adaptive.enabled" = "true"
        if ($StorageAccountHash) {
            foreach ($sa in $StorageAccountHash.Keys) {
                #Write-Host $sa, $StorageAccountHash.$sa
                $spark_conf.$sa = $StorageAccountHash.$sa
            }
        }
        $DefaultCluster.spark_conf = $spark_conf
    
        #init script 
        if($InitScriptPath -ne "") {
            $initscriptinfo = @{}
            $dbfsstorageinfo = @{}
            $dbfsstorageinfo.destination = $InitScriptPath
            $initscriptinfo.dbfs = $dbfsstorageinfo
            $DefaultCluster.init_scripts = $initscriptinfo
        }
        #endregion
    
        #region begin Maintenance Cluster 
        $MaintenanceCluster = @{}
        $MaintenanceCluster.label = "maintenance"
        $MaintenanceCluster.node_type_id = $NodeType
        $MaintenanceCluster.driver_node_type_id = $DriverNodeType
        $MaintenanceCluster.autotermination_minutes = $AutoTerminationMinutes
    
        #workers
        if($MinWorkers -eq 0) {
            $MaintenanceCluster.num_workers = $MaxWorkers
        } else {
            $autoscaleinfo = @{}
            $autoscaleinfo.min_workers = $MinWorkers
            $autoscaleinfo.max_workers = $MaxWorkers
            $autoscale = New-Object -TypeName PSObject -Property $autoscaleinfo
            $MaintenanceCluster.autoscale = $autoscale
        }
    
        #env_vars
        $spark_env_vars = @{}
        $spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"
        $MaintenanceCluster.spark_env_vars = $spark_env_vars
    
        #spark.conf
        $spark_conf = @{}
        $spark_conf."spark.scheduler.mode" = "FAIR"
        $spark_conf."spark.sql.adaptive.enabled" = "true"
        if ($StorageAccountHash) {
            foreach ($sa in $StorageAccountHash.Keys) {
                #Write-Host $sa, $StorageAccountHash.$sa
                $spark_conf.$sa = $StorageAccountHash.$sa
            }
        }
        $MaintenanceCluster.spark_conf = $spark_conf
    
        #init script 
        if($InitScriptPath -ne "") {
            $initscriptinfo = @{}
            $dbfsstorageinfo = @{}
            $dbfsstorageinfo.destination = $InitScriptPath
            $initscriptinfo.dbfs = $dbfsstorageinfo
            $MaintenanceCluster.init_scripts = $initscriptinfo
        }
        #endregion
    
        [System.Collections.ArrayList]$clusters = @()
        $clusters.Add($DefaultCluster)
        if($CreateMaintenanceCluster -eq $true) {$clusters.Add($MaintenanceCluster)}
        $PipelineSettings.clusters = $clusters 
        #endregion
        
        $PipelineSettings = New-Object -TypeName psobject -Property $PipelineSettings
        $PipelineJSON = $PipelineSettings | ConvertTo-Json -Depth 32
        $PipelineJSON
        Return Invoke-PostRESTRequest -URI "${BaseURI}/pipelines" -Token $Token -Body $PipelineJSON
    }
    else {
        Write-Host "DLT Pipeline Already Exists"
    }
}

Function Remove-DeltaLiveTablePipeline {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName
    )
    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {
        Return Invoke-DeleteRESTRequest -URI "${BaseURI}/pipelines/${pipeline_id}" -Token $Token 
    } else {
        Write-Host "Pipeline Does not exist."
    }
}

Function Get-DeltaLiveTablePipelineDetails {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName
    )
    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {
        Return Invoke-GetRESTRequest -URI "${BaseURI}/pipelines/${pipeline_id}" -Token $Token 
    } else {
        Write-Host "Pipeline Does not exist."
    }
}

Function Update-DeltaLiveTablePipeline {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName,
        [Parameter(Mandatory=$false, Position = 3)]
        [string]$Storage = "",
        [Parameter(Mandatory=$false, Position = 4)]
        [System.Collections.ArrayList]$PipelineNotebooks = @(),
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$Target = "",
        [Parameter(Mandatory=$true, Position = 6)]
        [bool]$Continuous = $false,
        [Parameter(Mandatory=$true, Position = 7)]
        [bool]$DevelopmentMode = $true,
        [Parameter(Mandatory=$false, Position = 8)]
        $ConfigurationHash = @{}
    )

    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id)
    {
        $spec = ((Get-DeltaLiveTablePipelineDetails -BaseURI $URIBase -Token $Token -PipelineName $PipelineName).Content | ConvertFrom-Json).spec
        if($ConfigurationHash.Count -gt 0) {$spec.configuration = $ConfigurationHash}
        if($Storage -ne "") {$spec.storage = $Storage}
        if($Target -ne "") {$spec.target = $Target}
        if($PipelineNotebooks.Count -gt 0) {$spec.libraries = $PipelineNotebooks}
        $spec.development = $DevelopmentMode
        $spec.continuous = $Continuous
        $specJSON = $spec | ConvertTo-Json -Depth 32
        $specJSON
        Return Invoke-PutRESTRequest -URI "${BaseURI}/pipelines/$pipeline_id" -Token $Token -Body $specJSON
    } else {
        Write-Host "Pipeline does not exist."
    }
}

Function Start-DeltaLiveTablePipeline {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName,
        [Parameter(Mandatory=$false, Position = 3)]
        [bool]$FullRefresh = $false
    )

    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {       
        $PipelineRequest = @{}
        $PipelineRequest.full_refresh = $FullRefresh
        $PipelineRequestJSON = $PipelineRequest | ConvertTo-Json
        Return Invoke-PostRESTRequest -URI "${BaseURI}/pipelines/$pipeline_id/updates" -Token $Token -Body $PipelineRequestJSON
    } else {
        Write-Host "Pipeline does not exist."
    }
}

Function Get-DeltaLiveTablePipelineUpdateDetails {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$UpdateId
    )
    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {
        Return Invoke-GetRESTRequest -URI "${BaseURI}/pipelines/$pipeline_id/updates/$updateId" -Token $Token
    } else {
        Write-Host "Pipeline does not exist."
    }
}

Function Get-DeltaLiveTablePipelineEvents {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName,
        [Parameter(Mandatory=$false, Position = 3)]
        [string]$PageToken = "",
        [Parameter(Mandatory=$false, Position = 4)]
        [string]$MaxResults = 100,
        [Parameter(Mandatory=$false, Position= 5)]
        [string]$OrderBy = "timestamp desc",
        [Parameter(Mandatory=$false, Position = 6)]
        [string]$Filter = ""
    )
    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {
        $Body = @{}
        if($PageToken -ne "") {$Body.page_token = $PageToken}
        $Body.max_results = $MaxResults 
        if($OrderBy -ne "") {$Body.order_by = $OrderBy} 
        if($Filter -ne "") {$Body.filter = $Filter}
        $BodyJSON = $Body | ConvertTo-Json
        Return Invoke-GetRESTRequest -URI "${BaseURI}/pipelines/$pipeline_id/events" -Token $Token -Body $BodyJSON
    } else {
        Write-Host "Pipeline does not exist."
    }
}

Function Stop-DeltaLiveTablePipeline {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PipelineName
    )

    $pipeline_id = ((Get-DeltaLiveTablePipelines -BaseURI $BaseURI -Token $Token -MaxResults 1 -Filter "name LIKE '$PipelineName'").Content | ConvertFrom-Json).statuses.pipeline_id
    if($pipeline_id) {       
        Return Invoke-PostRESTRequest -URI "${BaseURI}/pipelines/$pipeline_id/stop" -Token $Token
    } else {
        Write-Host "Pipeline does not exist."
    }
}