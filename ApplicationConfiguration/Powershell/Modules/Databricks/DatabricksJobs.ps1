. $PSScriptRoot\Helper.ps1

Function Get-DatabricksJobs {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token
    )

    return Invoke-GetRESTRequest -URI "${BaseURI}/jobs/list" -Token $Token
}

Function Get-DatabricksJob {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName
    )

    $jobdatajson = Get-DatabricksJobs -BaseURI $BaseURI -Token $Token
    $jobs = convertFrom-Json $jobdatajson.Content
    $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $JobName} | ConvertTo-Json
    return $jobdata
}

Function Remove-DatabricksJob {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName
    )

    $jobdata = Get-DatabricksJob -BaseURI $BaseURI -Token $Token -JobName $JobName
    If($jobdata) {
        $jobid = (ConvertFrom-Json $jobdata | Select-Object "job_id").job_id
        $jobinfo = @{}
        $jobinfo.job_id = $jobid
        $jobjson = $jobinfo | ConvertTo-Json

        Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/delete" -Token $Token -Body $jobjson
    } Else {
        Write-Host "Job does not exist"
    }
}

Function New-DatabricksJsonJob {
    param (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$JobJson
    )
    $jobdatajson = Get-DatabricksJobs -BaseURI $BaseURI -Token $Token
    $jobs = convertFrom-Json $jobdatajson.Content
    $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $JobName} | Select-Object "job_id" | ConvertTo-Json
    if(!$jobdata)
    {
        Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/create" -Token $Token -Body $JobJson
    }
}

Function New-NotebookTaskSettings {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$NotebookPath,
        [Parameter(Mandatory=$false, Position = 1)]
        [Hashtable]$BaseParameters = @{}
    )
    $NotebookTask = @{}
    $NotebookTask.notebook_path = $NotebookPath 
    $NotebookTask.base_parameters = $BaseParameters 
    $NotebookTaskJSON = $NotebookTask | ConvertTo-Json -Depth 32
    #Write-Host "$NotebookTaskJSON"
    Return $NotebookTaskJSON
}

Function New-PipelineTaskSettings {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$PipelineId
    )
    $PipelineTask = @{}
    $PipelineTask.pipeline_id = $PipelineId
    $PipelineTaskJSON = $PipelineTask | ConvertTo-Json -Depth 32
    #Write-Host "$PipelineTaskJSON"
    Return $PipelineTaskJSON
}

Function New-TaskSettings {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$TaskKey,
        [Parameter(Mandatory=$false, Position = 1)]
        [string]$Description = "",
        [Parameter(Mandatory=$false, Position = 2)]
        [System.Collections.ArrayList]$DependsOn = @(),
        [Parameter(Mandatory=$false, Position = 3)]
        [int]$TimeoutSeconds = 0,
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$MaxRetries = 0,
        [Parameter(Mandatory=$false, Position = 5)]
        [int]$MinRetryIntervalMilliseconds = 0,
        [Parameter(Mandatory=$false, Position = 6)]
        [bool]$RetryOnTimeout = $false,
        [Parameter(Mandatory=$false, Position = 7)]
        [System.Collections.ArrayList]$JobStartEmailNotification = @(),
        [Parameter(Mandatory=$false, Position = 8)]
        [System.Collections.ArrayList]$JobSuccessEmailNotification = @(),
        [Parameter(Mandatory=$false, Position = 9)]
        [System.Collections.ArrayList]$JobFailureEmailNotification = @(),
        [Parameter(Mandatory=$false, Position = 10)]
        [bool]$NoAlertForSkippedRuns = $false,
        [Parameter(Mandatory=$false, Position = 11)]
        [string]$ExistingClusterId = "",
        [Parameter(Mandatory=$false, Position = 12)]
        $NewClusterSettings = @{},
        [Parameter(Mandatory=$false, Position = 13)]
        $NotebookTaskSettings = @{},
        [Parameter(Mandatory=$false, Position = 14)]
        $PipelineTaskSettings = @{},
        [Parameter(Mandatory=$false, Position = 15)]
        [Object]$Libraries = @()
        #(sparkjar, sparkpython, sparksubmit not yet implemented)
    )

    if($ExistingClusterID -eq "" -and $NewClusterSettings.Count -eq 0 -and $NotebookTaskSettings.Count -gt 0) {throw("Cluster Details must be supplied.")}
    if($NotebookTaskSettings.Count -eq 0 -and $PipelineTaskSettings.Count -eq 0) {throw("Notebook or Pipeline details must be supplied.")}
    $TaskSettings = @{}
    $TaskSettings.task_key = $TaskKey
    if($Description -ne "") {$TaskSettings.description = $Description}
    if($DependsOn.Count -gt 0) {
        [System.Collections.ArrayList]$TaskKeyArrayList = @()
        foreach($task_key in $DependsOn) {
            $depends_on = @{}
            $depends_on.task_key = $task_key 
            $TaskKeyArrayList.add($depends_on) | Out-Null
        }
        $TaskSettings.depends_on = $TaskKeyArrayList
    }
    if ($null -ne $Libraries) {
        $TaskSettings.libraries = $Libraries
    }
    if($TimeoutSeconds -ne 0) {$TaskSettings.timeout_seconds = $TimeoutSeconds}
    if($MaxRetries -ne 0) {$TaskSettings.max_retries = $MaxRetries}
    if($MinRetryIntervalMilliseconds -ne 0) {$TaskSettings.min_retry_interval_millis = $MinRetryIntervalMilliseconds}
    $TaskSettings.retry_on_timeout = $RetryOnTimeout
    $JobEmailNotifications = @{}
    if($JobStartEmailNotification.Count -gt 0) {$JobEmailNotifications.on_start = $JobStartEmailNotification}
    if($JobSuccessEmailNotification.Count -gt 0) {$JobEmailNotifications.on_success = $JobSuccessEmailNotification}
    if($JobFailureEmailNotification.Count -gt 0) {$JobEmailNotifications.on_failure = $JobFailureEmailNotification}
    $JobEmailNotifications.no_alert_for_skipped_runs = $NoAlertForSkippedRuns
    $TaskSettings.email_notifications = $JobEmailNotifications
    if($ExistingClusterID -ne "") {$TaskSettings.existing_cluster_id = $ExistingClusterID}
    if($NewClusterSettings.Count -gt 0) {$TaskSettings.new_cluster = $NewClusterSettings}
    if($NotebookTaskSettings.Count -gt 0) {$TaskSettings.notebook_task = $NotebookTaskSettings}
    if($PipelineTaskSettings.Count -gt 0) {$TaskSettings.pipeline_task = $PipelineTaskSettings}
    
    $TaskSettingsJSON = $TaskSettings | ConvertTo-Json -Depth 32
    Return $TaskSettingsJSON
}

Function New-ClusterSettings {
    param 
    (
        [Parameter(Mandatory=$false, Position = 0)]
        [int]$MinWorkers = 0,
        [Parameter(Mandatory=$true, Position = 1)]
        [int]$MaxWorkers,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SparkVersion,
        [Parameter(Mandatory=$false, Position = 3)]
        [Hashtable]$SparkConf = @{},
        [Parameter(Mandatory=$true, Position = 4)]
        [string]$NodeType,
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$DriverNodeType = "",
        [Parameter(Mandatory=$false, Position = 6)]
        [string]$InstancePoolID = "",
        [Parameter(Mandatory=$false, Position = 7)]
        [string]$DriverInstancePoolID = "",
        [Parameter(Mandatory=$false, Position = 8)]
        [Hashtable]$CustomTags = @{},
        [Parameter(Mandatory=$false, Position = 9)]
        [string]$ClusterLogPath = "dbfs:/clusterlogs",
        [Parameter(Mandatory=$false, Position = 10)]
        [string]$InitScript = "",
        [Parameter(Mandatory=$false, Position = 11)]
        [Hashtable]$SparkEnvVars = @{},
        [Parameter(Mandatory=$false, Position = 12)]
        [bool]$EnableElasticDisk = $true
    )
    $NewCluster = @{}
    if ($MinWorkers -eq 0){
        $NewCluster.num_workers = $MaxWorkers
    } else {
        $AutoScale = @{}
        $AutoScale.min_workers = $MinWorkers
        $AutoScale.max_workers = $MaxWorkers
        $AutoScale = New-Object -TypeName PSObject -Property $AutoScale
        $NewCluster.autoscale = $AutoScale
    }
    $NewCluster.spark_version = $SparkVersion
    $NewCluster.spark_conf = $SparkConf
    if ($InstancePoolId -ne "") {
        $NewCluster.instance_pool_id = $InstancePoolId
        if($DriverInstancePoolID -ne "") {$NewCluster.driver_instance_pool_id = $DriverInstancePoolID}
    } else {
        $NewCluster.node_type_id = $NodeType
        if($DriverNodeType -ne "") {$NewCluster.driver_node_type_id = $DriverNodeType}
    }
    if($CustomTags.Count -gt 0) {$NewCluster.custom_tags = $CustomTags}
    $ClusterLogConf = @{}
    $DBFSStorageInfo = @{}
    $DBFSStorageInfo.destination = $ClusterLogPath
    $ClusterLogConf.dbfs = $DBFSStorageInfo
    $NewCluster.cluster_log_conf = $ClusterLogConf 
    if($InitScript -ne "") {
        $InitScriptInfo = @{}
        $DBFSStorageInfo = @{}
        $DBFSStorageInfo.destination = $InitScript
        $InitScriptInfo.dbfs = $DBFSStorageInfo
        $NewCluster.init_scripts = $InitScriptInfo
    }
    if($SparkEnvVars.Count -gt 0) {$NewCluster.spark_env_vars = $SparkEnvVars}
    $NewCluster.enable_elastic_disk = $EnableElasticDisk

    $NewClusterJSON = $NewCluster | ConvertTo-Json -Depth 32
    Write-Host "$NewClusterJSON"
    Return $NewClusterJSON
}

Function New-DatabricksMultiTaskJob {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName,
        [Parameter(Mandatory=$false, Position = 3)]
        [int]$MaxConcurrentRuns = 1,
        [Parameter(Mandatory=$true, Position = 4)]
        [System.Collections.ArrayList]$Tasks,
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$Format = 'MULTI_TASK'
    )
    $jobs = ((Get-DatabricksJobs -BaseURI $BaseURI -Token $Token) | ConvertFrom-Json).Content
    $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $JobName} | Select-Object "job_id" | ConvertTo-Json
    if(!$jobdata)
    {
        $MultiTaskJob = @{}
        $MultiTaskJob.name = $JobName 
        $MultiTaskJob.max_concurrent_runs = $MaxConcurrentRuns
        $MultiTaskJob.format = $Format
        $MultiTaskJob.tasks = $Tasks 
        $MultiTaskJobJSON = $MultiTaskJob | ConvertTo-Json -Depth 32
        Write-Host "$MultiTaskJobJSON"
        Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/create" -Token $Token -Body $MultiTaskJobJSON
        #Return $MultiTaskJobJSON
    } else {
        Write-Host "Job already exists"
    }
}

Function New-DatabricksJob {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName,
        [Parameter(Mandatory=$false, Position = 3)]
        [string]$TimezoneID = "",
        [Parameter(Mandatory=$false, Position = 4)]
        [string]$CronSchedule = "",
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$EmailNotification = "",
        [Parameter(Mandatory=$true, Position = 6)]
        [string]$NotebookPath,
        [Parameter(Mandatory=$true, Position = 7)]
        [string]$NodeType,
        [Parameter(Mandatory=$true, Position = 8)]
        [string]$SparkVersion,
        [Parameter(Mandatory=$false, Position = 9)]
        [int]$MinWorkers = 0,
        [Parameter(Mandatory=$true, Position = 10)]
        [int]$MaxWorkers,
        [Parameter(Mandatory=$false, Position = 11)]
        [string]$InitScript = "",
        [Parameter(Mandatory=$true, Position = 12)]
        [string]$InstancePoolID,
        [Parameter(Mandatory=$false, Position = 13)]
        [Object]$Libraries = @(),
        [Parameter(Mandatory=$false, Position = 14)]
        [Hashtable]$Parameters = @{},
        [Parameter(Mandatory=$false, Position = 15)]
        [int]$TimeoutSeconds = 6000,
        [Parameter(Mandatory=$false, Position = 16)]
        [int]$MaxRetries = 0,
        [Parameter(Mandatory=$false, Position = 17)]
        [int]$MaxConcurrentRuns = 1,
        [Parameter(Mandatory=$false, Position = 18)]
        $StorageAccountHash = @{}
    )

    $jobdatajson = Get-DatabricksJobs -BaseURI $BaseURI -Token $Token
    $jobs = convertFrom-Json $jobdatajson.Content
    $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $JobName} | Select-Object "job_id" | ConvertTo-Json
    if(!$jobdata)
    {
        $jobinfo = @{}
        $newclusterinfo = @{}
        $jobemailinfo = @{}
        $jobscheduleinfo = @{}
        $notebooktaskinfo = @{}

        $spark_conf = @{}
        $spark_conf."spark.scheduler.mode" = "FAIR"
        if ($StorageAccountHash) {
            foreach ($sa in $StorageAccountHash.Keys) {
                Write-Host $sa, $StorageAccountHash.$sa
                $spark_conf.$sa = $StorageAccountHash.$sa
            }
        }

        $spark_env_vars = @{}
        $spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"

        if($InitScript -ne "")
        {
                $initscriptinfo = @{}
                $dbfsstorageinfo = @{}
                $dbfsstorageinfo.destination = $InitScript
                $initscriptinfo.dbfs = $dbfsstorageinfo
                $newclusterinfo.init_scripts = $initscriptinfo
        }

        if($MinWorkers -eq 0){
            $newclusterinfo.num_workers = $MaxWorkers
        } else {
            $autoscaleinfo = @{}
            $autoscaleinfo.min_workers = $MinWorkers
            $autoscaleinfo.max_workers = $MaxWorkers
            $autoscale = New-Object -TypeName PSObject -Property $autoscaleinfo
            $newclusterinfo.autoscale = $autoscale
        }

        $newclusterinfo.spark_version = $SparkVersion
        $newclusterinfo.spark_conf = $spark_conf

        if ($InstancePoolId -ne "") {
            $newclusterinfo.instance_pool_id = $InstancePoolId
        } else {
            $newclusterinfo.node_type_id = $NodeType
            $newclusterinfo.driver_node_type_id = $DriverNodeType
        }

        $notebooktaskinfo.notebook_path = $NotebookPath
        $notebooktaskinfo.base_parameters = $Parameters

        If($EmailNotification -ne "") {
            $jobemailinfo.on_failure = $EmailNotification
        }

        If($CronSchedule -ne "") {
            $jobscheduleinfo.quartz_cron_expression = $CronSchedule
            $jobscheduleinfo.timezone_id = $TimezoneID
            $jobinfo.schedule = $jobscheduleinfo
        }

        if ($null -ne $Libraries) {
            $jobinfo.libraries = $Libraries
        }

        $jobinfo.new_cluster = $newclusterinfo
        $jobinfo.notebook_task = $notebooktaskinfo
        $jobinfo.name = $JobName
        $jobinfo.email_notifications = $jobemailinfo
        $jobinfo.timeout_seconds = $TimeoutSeconds
        $jobinfo.max_retries = $MaxRetries
        $jobinfo.max_concurrent_runs = $MaxConcurrentRuns
        $jobjson = $jobinfo | ConvertTo-Json -Depth 32
        Write-Host "$jobjson"
        Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/create" -Token $Token -Body $jobjson
    } Else {
        Write-Host "Job already exists"
    }
}

Function Invoke-DatabricksJob {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName
    )

    $jobdata = (Get-DatabricksJob -BaseURI $BaseURI -Token $Token -JobName $JobName) | ConvertFrom-Json
    If($jobdata)
    {
        $jobinfo = @{}
        $jobinfo.job_id = $jobdata.job_id
        $jobjson = $jobinfo | ConvertTo-Json -Depth 32

        Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/run-now" -Token $Token -Body $jobjson
    } else {
        Write-Host "Job does not exist"
    }
}

Function Get-DatabricksJobRuns {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [int64]$JobID,
        [Parameter(Mandatory=$false, Position = 3)]
        [boolean]$ActiveOnly = $false,
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$Offset = 0,
        [Parameter(Mandatory=$false, Position = 5)]
        [int]$Limit = 150
    )

    If($JobID -eq 0) {
        $job = ""
    } Else{
        $job = "job_id=${JobID}&"
    }
    If($ActiveOnly -eq $false) {
        $activeonlystring = "false"
    } else{
        $activeonlystring = "true"
    }

    Return Invoke-GetRESTRequest -URI "${BaseURI}/jobs/runs/list?active_only=${activeonlystring}&${job}${Offset}=0&limit=${Limit}" -Token $Token
}

Function Get-DatabricksJobRun {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [int]$RunID
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/jobs/runs/get?run_id=$RunId" -Token $Token 
}

Function Get-DatabricksJobRunOutput {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [int]$RunID
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/jobs/runs/get-output?run_id=$RunID" -Token $Token
}

Function Get-DatabricksJobRunsStatus {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobName,
        [Parameter(Mandatory=$true, Position = 3)]
        [boolean]$WaitForCompletion
    )

    $jobdetail = Get-DatabricksJob -BaseURI $BaseURI -Token $Token -JobName $JobName
    If($jobdetail)
    {
        $jobid = ($jobdetail | ConvertFrom-Json).job_id
        $jobrunsjson = Get-DatabricksJobRuns -BaseURI $BaseURI -Token $Token -JobID $jobid -ActiveOnly $false
        $runs = $jobrunsjson.Content | ConvertFrom-Json

        If($WaitForCompletion -eq $false) {
            $runs.runs | Format-Table
        } Else {
            $latestrun = $runs.runs[0] #note this logic doesn't currently support concurrent runs (just getting latest run)
            $latestrun | Format-Table
            $lifecyclestate = $latestrun.state.life_cycle_state
            While($lifecyclestate -notin "TERMINATED", "SKIPPED", "INTERNAL_ERROR")
            {
                #wait 30 seconds and check again until a terminal status is reached
                Start-Sleep -Seconds 30
                $jobrunsjson = Get-DatabricksJobRuns -BaseURI $BaseURI -Token $Token -JobID $jobid -ActiveOnly $false
                $runs = $jobrunsjson.Content | ConvertFrom-Json
                $latestrun = $runs.runs[0]
                $latestrun | Format-Table
                $lifecyclestate = $latestrun.state.life_cycle_state
            }
        }
    } Else {
        Write-Host "Job does not exist"
    }
}

Function Start-DatabricksNotebook {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$BaseURI,
        [Parameter(Mandatory=$true, Position = 1)]
        [securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ExistingClusterID,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$NotebookPath,
        [Parameter(Mandatory=$true, Position = 4)]
        [object]$NotebookParams,
        [Parameter(Mandatory=$true, Position = 5)]
        [string]$RunName
    )

    $jobInfo = @{}
    $notebookTaskInfo = @{}
    $jobInfo.existing_cluster_id = $ExistingClusterID
    $notebookTaskInfo.notebook_path = $NotebookPath
    $notebookTaskInfo.base_parameters = $NotebookParams
    $notebookTaskjson = New-Object -TypeName PSObject -Property $notebookTaskInfo

    $jobInfo.notebook_task = $notebookTaskjson
    $jobInfo.run_name = $RunName
    $jobInfojson = New-Object -TypeName PSObject -Property $jobInfo | ConvertTo-Json

    Return Invoke-PostRESTRequest -URI "${BaseURI}/jobs/runs/submit" -Token $Token -Body $jobInfojson
}