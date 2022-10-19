function get-DatabricksSecretScopes
{
        $uri = "$uribase/secrets/scopes/list"
        $response = Invoke-WebRequest -uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Secret Scopes Failed" -ForegroundColor Red
        }
        return $response
}

function new-DatabricksSecretScope ($scopeName)
{
        $scopeInfo = @{}
        $scopeInfo.scope = [string]$scopeName
        $scopeInfo.initial_manage_principal = [string]"users"
        $scopeInfojson = New-Object -TypeName PSObject -Property $scopeInfo | ConvertTo-Json

        $uri = "$uribase/secrets/scopes/create"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $scopeInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Create Secret Scope Failed" -ForegroundColor Red
        }
        return $response
}

function add-DatabricksSecretScope ($scopeName)
{
    $scopes = get-DatabricksSecretScopes
    $scopeNames = ($scopes.Content | ConvertFrom-Json).scopes.name

    if($scopeName -notin $scopeNames)
    {
        $scopecreationresponse = new-DatabricksSecretScope $scopeName
        write-host $scopecreationresponse
    }
}

function remove-DatabricksSecretScope ($scope)
{
        $bodyHash = @{}
        $bodyHash.scope = $scope
        $bodyObject = New-Object –TypeName PSObject –Property $bodyHash
        $body = $bodyObject | ConvertTo-Json

        $uri = "$uribase/secrets/scopes/delete"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $body
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Deleting Secret Scope Failed" -ForegroundColor Red
        }
        return $response
}

function get-DatabricksSecrets ($scopeName)
{
        $secretsInfo = @{}
        $secretsInfo.scope = [string]$scopeName
        $scopeInfoJson = New-Object –TypeName PSObject –Property $secretsInfo | ConvertTo-Json

        $uri = "$uribase/secrets/list"
        $response = Invoke-WebRequest -uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $scopeInfoJson

        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Secrets Failed" -ForegroundColor Red
        }
        return $response
}

function add-DatabricksSecret ($stringValue, $secretScope, $secretKey)
{
        $secretInfo = @{}
        $secretInfo.string_value = [string]$stringValue
        $secretInfo.scope = [string]$secretScope
        $secretInfo.key = [string]$secretKey
        $secretInfojson = New-Object -TypeName PSObject -Property $secretInfo | ConvertTo-Json

        $uri = "$uribase/secrets/put"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $secretInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Create Secret Failed" -ForegroundColor Red
        }
        return $response
}

function get-DatabricksGroups
{
        $uri = "$uribase/groups/list"
        $response = Invoke-WebRequest -uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Groups Failed" -ForegroundColor Red
        }
        return $response
}

function new-DatabricksGroup ($groupName)
{
        $groupInfo = @{}
        $groupInfo.group_name = [string]$groupName
        $groupInfojson = New-Object –TypeName PSObject –Property $groupInfo | ConvertTo-Json

        $uri = "$uribase/groups/create"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $groupInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Create Group Failed" -ForegroundColor Red
        }
        return $response
}

function add-DatabricksGroup ($groupName)
{
    $groups = get-DatabricksGroups
    $groupNames = ($groups.Content | ConvertFrom-Json).group_names

    if($groupName -notin $groupNames)
    {
        $groupcreationresponse = new-DatabricksGroup $groupName
        write-host $groupcreationresponse
    }
}

function get-DatabricksGroupMembers ($groupName)
{
        $groupInfo = @{}
        $groupInfo.group_name = [string]$groupName
        $groupInfojson = New-Object –TypeName PSObject –Property $groupInfo | ConvertTo-Json

        $uri = "$uribase/groups/list-members"
        $response = Invoke-WebRequest -uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $groupInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Group Members Failed" -ForegroundColor Red
        }
        return $response
}

function add-DatabricksGroupMembers($groupName, $memberNames, $userType)
{
        $members = $memberNames.Split(" ")
        foreach ($memberName in $members)
        {
                add-DatabricksGroupMember $groupName $memberName $userType
        }
}
function new-DatabricksGroupMember ($Name, $type="GROUP", $parentName)
{
        $groupInfo = @{}
        if($type -eq "USER"){$groupInfo.user_name = $Name}
        if($type -eq "GROUP"){$groupInfo.group_name = $Name}
        $groupInfo.parent_name = $parentName
        $groupInfojson = New-Object –TypeName PSObject –Property $groupInfo | ConvertTo-Json

        $uri = "$uribase/groups/add-member"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $groupInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Add Member Failed" -ForegroundColor Red
        }
        return $response
}

function add-DatabricksGroupMember($groupName, $memberName, $userType)
{
    $groupsList = get-DatabricksGroupMembers $groupName
    $groupsListmembers = ($groupsList.Content | ConvertFrom-Json).members

    if($memberName -notin $groupsListmembers.user_name)
    {
            $memberCreationResponse=new-DatabricksGroupMember $memberName $userType $groupName
            write-host $memberCreationResponse
    }
}

function get-DatabricksClusters
{
        $clusterlisturi = "$uribase/clusters/list"
        $clustersjson = Invoke-WebRequest -Uri $clusterlisturi `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        return $clustersjson
}

function remove-DatabricksCluster($clustername)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id"
        if($clusterdata)
        {
                $clusterinfo = @{}
                $clusterinfo.cluster_id = $clusterdata.cluster_id
                $clusterjson = $clusterinfo | ConvertTo-Json
                $clusterjson
                $deleteclusteruri = "$uribase/clusters/permanent-delete"
                $createclusterresponse = Invoke-WebRequest -Uri $deleteclusteruri `
                        -Method Post `
                        -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $clusterjson
                if ($createclusterresponse.StatusCode -ne 200)
                {
                        Write-Host "Delete Cluster Failed" -ForegroundColor Red
                }
                return $createclusterresponse
        }
        else {
                Write-Host "cluster does not exist"
        }
}

function new-DatabricksCluster($clustername)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json
        if(!$clusterdata)
        {
                $autoscaleinfo = @{}
                $autoscaleinfo.min_workers = $min_workers
                $autoscaleinfo.max_workers = $max_workers
                $autoscale = New-Object –TypeName PSObject –Property $autoscaleinfo
                $spark_env_vars = @{}
                $spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"

                $initscriptinfo = @{}
                $dbfsstorageinfo = @{}
                $dbfsstorageinfo.destination = $initScript
                $initscriptinfo.dbfs = $dbfsstorageinfo
                #$initscriptinfo | ConvertTo-Json

                $clusterinfo = @{}
                $clusterinfo.cluster_name = $clustername
                $clusterinfo.spark_version = $sparkversion
                $spark_conf = @{}
                $spark_conf."spark.scheduler.mode" = "FAIR"
                $clusterinfo.spark_conf = $spark_conf
                $clusterinfo.node_type_id = $nodetype
                $clusterinfo.driver_node_type_id = $drivernodetype
                $clusterinfo.autoscale = $autoscale
                $clusterinfo.spark_env_vars = $spark_env_vars
                $clusterinfo.init_scripts = $initscriptinfo
                $clusterinfo.autotermination_minutes = $autoterminationminutes
                $clusterinfo = New-Object -TypeName psobject -Property $clusterinfo
                $clusterjson = $clusterinfo | ConvertTo-Json
                $clusterjson

                $createclusteruri = "$uribase/clusters/create"
                $createclusterresponse = Invoke-WebRequest -Uri $createclusteruri `
                        -Method Post `
                        -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $clusterjson

                if ($createclusterresponse.StatusCode -ne 200)
                {
                        Write-Host "Create Cluster Failed" -ForegroundColor Red
                }
                return $createclusterresponse
        }
        else {
                Write-Host "cluster already exists"
        }
}

function get-DatabricksJobs
{
        $jobslisturi = "$uribase/jobs/list"
        $jobsjson = Invoke-WebRequest -Uri $jobslisturi `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        return $jobsjson
}

function get-DatabricksJob ($jobname)
{
        $jobdatajson = get-DatabricksJobs
        $jobs = convertFrom-Json $jobdatajson.Content
        $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $jobname} | ConvertTo-Json
        return $jobdata
}

function remove-DatabricksJob ($jobname)
{
        $jobdata = get-DatabricksJob $jobname
        if($jobdata)
        {
                $jobid = (ConvertFrom-Json $jobdata | Select-Object "job_id").job_id
                $jobinfo = @{}
                $jobinfo.job_id = $jobid
                $jobjson = $jobinfo | ConvertTo-Json
                $deletejoburi = "$uribase/jobs/delete"
                $deletejobresponse = Invoke-WebRequest -Uri $deletejoburi `
                        -Method Post `
                        -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $jobjson
                if ($deletejobresponse.StatusCode -ne 200)
                {
                        Write-Host "Delete Job Failed" -ForegroundColor Red
                }
                return $deletejobresponse

        } else {
                Write-Host "Job does not exist"
        }
}

function new-DatabricksJob ($jobname, $notebookpath)
{
        $jobdatajson = get-DatabricksJobs
        $jobs = convertFrom-Json $jobdatajson.Content
        $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $jobname} | Select-Object "job_id" | ConvertTo-Json
        if(!$jobdata)
        {
                $createjoburi = "$uribase/jobs/create"
                $jobinfo = @{}
                $newclusterinfo = @{}
                $jobemailinfo = @{}
                $jobscheduleinfo = @{}
                $notebooktaskinfo = @{}

                $spark_conf = @{}
                $spark_conf."spark.scheduler.mode" = "FAIR"

                if($initscript -ne "")
                {
                        $initscriptinfo = @{}
                        $dbfsstorageinfo = @{}
                        $dbfsstorageinfo.destination = $initScript
                        $initscriptinfo.dbfs = $dbfsstorageinfo
                        $newclusterinfo.init_scripts = $initscriptinfo
                }

                $newclusterinfo.num_workers = $max_workers
                $newclusterinfo.spark_version = $sparkversion
                $newclusterinfo.spark_conf = $spark_conf
                $newclusterinfo.node_type_id = $nodetype


                $notebooktaskinfo.notebook_path = $notebookpath
                $baseparameterskvpair = @{}
                #example parameters
                #$baseparameterskvpair.systemExecutionGroupName = $jobexecutionparametervalue
                #$baseparameterskvpair.startDate = "2019-08-01"
                #$baseparameterskvpair.endDate = "2019-08-10"
                $notebooktaskinfo.base_parameters = $baseparameterskvpair

                $jobemailinfo.on_failure = $emailnotification #also on_start, on_success

                if($cronschedule -ne "")
                {
                        $jobscheduleinfo.quartz_cron_expression = $cronschedule
                        $jobscheduleinfo.timezone_id = $timezoneid
                        $jobinfo.schedule = $jobscheduleinfo
                }

                $jobinfo.new_cluster = $newclusterinfo
                $jobinfo.notebook_task = $notebooktaskinfo
                $jobinfo.name = $jobname
                $jobinfo.email_notifications = $jobemailinfo
                $jobinfo.timeout_seconds = 6000
                $jobinfo.max_retries = 0
                $jobinfo.max_concurrent_runs = 1
                $jobjson = $jobinfo | ConvertTo-Json -Depth 32
                $jobjson
                $createjobresponse = Invoke-WebRequest -Uri $createjoburi `
                        -Method Post `
                       -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $jobjson

                if ($createjobresponse.StatusCode -ne 200)
                {
                        Write-Host "Create Job Failed" -ForegroundColor Red
                }
                return $createjobresponse
        } else {
                Write-Host "job already exists"
        }
}

function new-DatabricksWorkspaceFolder ($path)
{
        $workspaceInfo = @{}
        $workspaceInfo.path = $path
        $workspaceInfojson = New-Object –TypeName PSObject –Property $workspaceInfo | ConvertTo-Json

        $uri = "$uribase/workspace/mkdirs"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $workspaceInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Create Folder Failed" -ForegroundColor Red
        }
        return $response
}

function import-notebook ($path, $format="SOURCE", $language="SOURCE", $localdirectory, [boolean]$overwrite=$false)
{
        $base64string = [Convert]::ToBase64String([IO.File]::ReadAllBytes($localdirectory))
        $importnotebookinfo = @{}
        $importnotebookinfo.path = $path
        $importnotebookinfo.format = $format
        $importnotebookinfo.language = $language
        $importnotebookinfo.content = $base64string
        $importnotebookinfo.overwrite = $overwrite
        $importnotebookjson = New-Object –TypeName PSObject –Property $importnotebookinfo | ConvertTo-Json

        $importtnotebookuri = "$uribase/workspace/import"
        $importnotebookresponse = Invoke-WebRequest -uri $importtnotebookuri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $importnotebookjson
        if ($importnotebookresponse.StatusCode -ne 200)
        {
                Write-Host "Import Notebook Failed" -ForegroundColor Red
        }
        return $importnotebookresponse
}

function import-NotebookDirectory($localpath, $workspacepath)
{
    $notebooks = Get-ChildItem $localpath | Where-Object {$_.Mode -ne "d-----"}
    foreach ($notebook in $notebooks)
    {
        $notebookName = $notebook.Name
        $notebookDestinationName = $notebookName.replace(".py", "")
        write-host $notebook $notebookName $notebookDestinationName
        $importnotebookresponse = import-notebook "$workspacepath/$notebookDestinationName" "SOURCE" "PYTHON" $notebook $true
        $importnotebookresponse
    }
}

function invoke-DatabricksJob ($jobname)
{
        $jobdata = (get-DatabricksJob $jobname) | ConvertFrom-Json
        if($jobdata)
        {
                $runjoburi = "$uribase/jobs/run-now"
                $jobinfo = @{}
                $jobinfo.job_id = $jobdata.job_id
                $jobjson = $jobinfo | ConvertTo-Json -Depth 32
                $jobjson
                $runjobresponse = Invoke-WebRequest -Uri $runjoburi `
                        -Method Post `
                        -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $jobjson

                if ($runjobresponse.StatusCode -ne 200)
                {
                        Write-Host "Run Job Failed" -ForegroundColor Red
                }
                return $runjobresponse
        } else {
                Write-Host "Job does not exist"
        }
}

function get-DatabricksJobRuns ($jobid=0, $activeonly=$false, $offset=0, $limit=150)
{
        if($jobid -eq 0) {$job=""} else{$job="job_id=$jobid&"}
        if($activeonly -eq $false) {$activeonlystring="false"} else{$activeonlystring="true"}
        $jobrunslisturi = "$uribase/jobs/runs/list?active_only=$activeonlystring&$job$offset=0&limit=$limit"
        $jobrunssjson = Invoke-WebRequest -Uri $jobrunslisturi `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        return $jobrunssjson
}

function get-DatabricksJobRunsStatus($jobname, $waitforcompletion=$false)
{
    $jobdetail = get-DatabricksJob $jobname
    if($jobdetail)
    {
        $jobid = ($jobdetail | ConvertFrom-Json).job_id
        $jobrunsjson = get-DatabricksJobRuns $jobid $false
        $runs = $jobrunsjson.Content | ConvertFrom-Json

        if ($waitforcompletion -eq $false)
        {
            $runs.runs | Format-Table
        } else {
            $latestrun = $runs.runs[0] #note this logic doesn't currently support concurrent runs (just getting latest run)
            $latestrun | Format-Table
            $lifecyclestate = $latestrun.state.life_cycle_state
            while($lifecyclestate -notin "TERMINATED", "SKIPPED", "INTERNAL_ERROR")
            {
                #wait 30 seconds and check again until a terminal status is reached
                Start-Sleep -Seconds 30
                $jobrunsjson = get-DatabricksJobRuns $jobid $false
                $runs = $jobrunsjson.Content | ConvertFrom-Json
                $latestrun = $runs.runs[0]
                $latestrun | Format-Table
                $lifecyclestate = $latestrun.state.life_cycle_state
            }
        }
    } else {
        Write-Host "Job does not exist"
    }
}

function start-DatabricksCluster ($clustername)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json
        if($clusterdata)
        {
                if($clusters.clusters.state -eq "TERMINATED")
                {
                        $startclusteruri = "$uribase/clusters/start"
                        $startclusterresponse = Invoke-WebRequest -Uri $startclusteruri `
                                -Method Post `
                                -Authentication Bearer `
                                -Token $token `
                                -ContentType "application/json" `
                                -Body $clusterdata
                        if ($startclusterresponse.StatusCode -ne 200)
                        {
                                Write-Host "Start Cluster Failed" -ForegroundColor Red
                        }
                        return $startclusterresponse
                }
                else {
                        Write-Host "Cluster is running"
                }
        }
        else {
                Write-Host "Cluster not found"
        }
}
function stop-DatabricksCluster ($clustername, $permanent=$false)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json

        if($permanent -eq $false)
        {$deleteclusteruri = "$uribase/clusters/delete"}
        else {$deleteclusteruri = "$uribase/clusters/permanent-delete"}

        $deleteclusterresponse = Invoke-WebRequest -Uri $deleteclusteruri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $clusterdata
        if ($deleteclusterresponse.StatusCode -ne 200)
        {
                Write-Host "Delete Cluster Failed" -ForegroundColor Red
        }
        return $deleteclusterresponse
}

function get-DatabricksWorkbookDirectoryContents ($notebookdirectory)
{
        $getdirectorycontentsuri = "$uribase/workspace/list"

        $getdirectorycontentsbodyinfo = @{}
        $getdirectorycontentsbodyinfo.path = $notebookdirectory
        $getdirectorycontentsbody = New-Object –TypeName PSObject –Property $getdirectorycontentsbodyinfo
        $getdirectorycontentsjson = $getdirectorycontentsbody | ConvertTo-Json

        $getdirectorycontentsresponse = Invoke-WebRequest -uri $getdirectorycontentsuri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $getdirectorycontentsjson

        if ($getdirectorycontentsresponse.StatusCode -ne 200)
        {
                Write-Host "Get Directory Contents Failed" -ForegroundColor Red
        }
        return $getdirectorycontentsresponse
}

function get-DatabricksClusterDetail ($clustername)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json
        $cluster_id = ($clusterdata | ConvertFrom-Json).cluster_id
        $getclusteruri = "$uribase/clusters/get?cluster_id=$cluster_id"
        $getclusterresponse = Invoke-WebRequest -Uri $getclusteruri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"

        if ($getclusterresponse.StatusCode -ne 200)
        {
                Write-Host "Get Cluster Failed" -ForegroundColor Red
        }
        return $getclusterresponse
}

function export-notebook ($path, $format, [boolean]$direct_download=$true, $outputpath)
{
        $exportnotebookinfo = @{}
        $exportnotebookinfo.path = $path
        $exportnotebookinfo.format = $format
        $exportnotebookjson = New-Object –TypeName PSObject –Property $exportnotebookinfo | ConvertTo-Json

        $exportnotebookuri = "$uribase/workspace/export?path=$path"
        if ($direct_download)
        {
                $exportnotebookuri += "&direct_download=true"
        }

        $exportnotebookresponse = Invoke-WebRequest -uri $exportnotebookuri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $exportnotebookjson `
                -OutFile $outputpath
        $exportnotebookresponse
}

function stop-RunningDatabricksClusters {
        $clustersjson = get-DatabricksClusters
        $clusters = $clustersjson | ConvertFrom-Json
        $clusters = $clusters.clusters
        foreach ($cluster in $clusters) {
                if ($cluster.state -ne "TERMINATED") {
                        stop-DatabricksCluster -clustername $cluster.cluster_name
                }
        }
}

function remove-DatabricksWorkspaceFolder ($path, $recursive=$true)
{
        $workspaceInfo = @{}
        $workspaceInfo.path = [string]$path
        $workspaceInfo.recursive = $recursive
        $workspaceInfojson = New-Object –TypeName PSObject –Property $workspaceInfo | ConvertTo-Json
        Write-Host $workspaceInfojson

        $uri = "$uribase/workspace/delete"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $workspaceInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Delete Folder Failed" -ForegroundColor Red
        }
        return $response
}

function remove-DatabricksGroup ($groupName)
{
        $groupInfo = @{}
        $groupInfo.group_name = [string]$groupName
        $groupInfojson = New-Object –TypeName PSObject –Property $groupInfo | ConvertTo-Json

        $uri = "$uribase/groups/delete"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $groupInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Delete Group Failed" -ForegroundColor Red
        }
        return $response
}

function remove-DatabricksGroupMember ($Name, $type="GROUP", $parentName)
{
        $groupInfo = @{}
        if($type -eq "USER"){$groupInfo.user_name = $Name}
        if($type -eq "GROUP"){$groupInfo.group_name = $Name}
        $groupInfo.parent_name = $parentName
        $groupInfojson = New-Object –TypeName PSObject –Property $groupInfo | ConvertTo-Json

        $uri = "$uribase/groups/remove-member"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $groupInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Remove Group Member Failed" -ForegroundColor Red
        }
        return $response
}

function Start-DatabricksNotebook ($existing_cluster_id, $NotebokPath, $notebook_params, $runName)
{
        $jobInfo = @{}
        $notebookTaskInfo = @{}
        $jobInfo.existing_cluster_id = $existing_cluster_id
        $notebookTaskInfo.notebook_path = $NotebokPath
        #$notebookTaskInfo.base_parameters = @{}
        $notebookTaskInfo.base_parameters = $notebook_params
        #$notebookTaskInfo.base_parameters = @{'"notebook_params":{"rawDataPath":"/mnt/training/zips.json","tableName":"zips","numPartitions":"8"}'}
        #$notebookTaskInfo.base_parameters = @{"notebook_params":{"rawDataPath":"/mnt/training/zips.json","tableName":"zips","numPartitions":"8"}}
        $notebookTaskjson = New-Object -TypeName PSObject -Property $notebookTaskInfo #| ConvertTo-Json
        $jobInfo.notebook_task = $notebookTaskjson
        $jobInfo.run_name = $runName
        $jobInfojson = New-Object –TypeName PSObject –Property $jobInfo | ConvertTo-Json
        write-host $jobInfojson

        $uri = "$uribase/jobs/runs/submit"
        $response = Invoke-WebRequest -uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $jobInfojson
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Start Databricks Notebook Failed" -ForegroundColor Red
        }
        return $response
}

function set-DatabricksSecretACLForPrincipal($scope, $principal, $permission)
{
        $bodyHash = @{}
        $bodyHash.scope = $scope
        $bodyHash.principal = $principal
        $bodyHash.permission = $permission
        $bodyObject = New-Object –TypeName PSObject –Property $bodyHash
        $body = $bodyObject | ConvertTo-Json

        $uri = "$uribase/secrets/acls/put"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $body
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Set Secret ACL for Principal Failed" -ForegroundColor Red
        }
        return $response
}

function get-DatabricksSecretACLForPrincipal($scope, $principal)
{
        $bodyHash = @{}
        $bodyHash.scope = $scope
        $bodyHash.principal = $principal
        $bodyObject = New-Object –TypeName PSObject –Property $bodyHash
        $body = $bodyObject | ConvertTo-Json

        $uri = "$uribase/secrets/acls/get"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $body
        if ($response.StatusCode -ne 200)
        {
                Write-Host "Get Secret ACL for Principal Failed" -ForegroundColor Red
        }
        return $response
}

function get-DatabricksSecretACLs($scope)
{
        $bodyHash = @{}
        $bodyHash.scope = $scope
        $bodyObject = New-Object –TypeName PSObject –Property $bodyHash
        $body = $bodyObject | ConvertTo-Json

        $uri = "$uribase/secrets/acls/list"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $body
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Secret ACLs Failed" -ForegroundColor Red
        }
        return $response
}

function get-ClusterLibraryStatuses
{
        $uri = "$uribase/libraries/all-cluster-statuses"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Cluster Library Statuses Failed" -ForegroundColor Red
        }
        return $response
}

function get-ClusterLibraryStatus($clusterid)
{
        $uri = "$uribase/libraries/cluster-status?cluster_id=$clusterid"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Cluster Library Statuses Failed" -ForegroundColor Red
        }
        return $response
}

function remove-ClusterMavenLibrary($clustername, $mavenCoordinate)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id").cluster_id
        if($clusterid)
        {
                $installedlibraries = get-ClusterLibraryStatus $clusterid
                $found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.maven | Where-Object {$_.coordinates -eq $mavenCoordinate}
                if($found)
                {
                        $body = @{}
                        $body.cluster_id = $clusterid
                        $libraries = @{}
                        $maven = @{}
                        $maven.coordinates = $mavenCoordinate
                        $libraries.maven = $maven
                        $body.libraries = $libraries
                        $bodyjson = $body | ConvertTo-Json
                        $bodyjson

                        $installlibraryuri = "$uribase/libraries/uninstall"
                        $installlibraryresponse = Invoke-WebRequest -Uri $installlibraryuri `
                                -Method Post `
                                -Authentication Bearer `
                                -Token $token `
                                -ContentType "application/json" `
                                -Body $bodyjson

                        if ($installlibraryresponse.StatusCode -ne 200)
                        {
                                Write-Host "Install Library Failed" -ForegroundColor Red
                        }
                        return $installlibraryresponse
                }
                else{
                        Write-Host "library not installed"
                }
        }
        else {
                Write-Host "cluster does not exist"
        }
}
function new-ClusterMavenLibrary($clustername, $mavenCoordinate)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id").cluster_id
        if($clusterid)
        {
                $installedlibraries = get-ClusterLibraryStatus $clusterid
                $found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.maven | Where-Object {$_.coordinates -eq $mavenCoordinate}
                if(!$found)
                {
                        $body = @{}
                        $body.cluster_id = $clusterid
                        $libraries = @{}
                        $maven = @{}
                        $maven.coordinates = $mavenCoordinate
                        $libraries.maven = $maven
                        $body.libraries = $libraries
                        $bodyjson = $body | ConvertTo-Json
                        $bodyjson

                        $installlibraryuri = "$uribase/libraries/install"
                        $installlibraryresponse = Invoke-WebRequest -Uri $installlibraryuri `
                                -Method Post `
                                -Authentication Bearer `
                                -Token $token `
                                -ContentType "application/json" `
                                -Body $bodyjson

                        if ($installlibraryresponse.StatusCode -ne 200)
                        {
                                Write-Host "Install Library Failed" -ForegroundColor Red
                        }
                        return $installlibraryresponse
                }
                else {
                        Write-Host "Library is already installed"
                }
        }
        else {
                Write-Host "cluster does not exist"
        }
}

function import-LocalFileToDBFS ($path, $localfile, [boolean]$overwrite=$false)
{
        $base64string = [Convert]::ToBase64String([IO.File]::ReadAllBytes($localfile))
        $body = @{}
        $body.path = $path
        $body.contents = $base64string
        $body.overwrite = $overwrite
        $bodyjson = New-Object –TypeName PSObject –Property $body | ConvertTo-Json

        $importfileuri = "$uribase/dbfs/put"
        $importfileresponse = Invoke-WebRequest -uri $importfileuri `
                -Method Post `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json" `
                -Body $bodyjson
        if ($importfileresponse.StatusCode -ne 200)
        {
                Write-Host "Import File Failed" -ForegroundColor Red
        }
        return $importfileresponse
}

function new-ClusterJarLibrary($clustername, $jarpath)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id").cluster_id
        if($clusterid)
        {
                $installedlibraries = get-ClusterLibraryStatus $clusterid
                $found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library | Where-Object {$_.jar -eq $jarpath}
                if(!$found)
                {
                        $body = @{}
                        $body.cluster_id = $clusterid
                        $libraries = @{}
                        $jar = $jarpath
                        $libraries.jar = $jar
                        $body.libraries = $libraries
                        $bodyjson = $body | ConvertTo-Json
                        $bodyjson

                        $installlibraryuri = "$uribase/libraries/install"
                        $installlibraryresponse = Invoke-WebRequest -Uri $installlibraryuri `
                                -Method Post `
                                -Authentication Bearer `
                                -Token $token `
                                -ContentType "application/json" `
                                -Body $bodyjson

                        if ($installlibraryresponse.StatusCode -ne 200)
                        {
                                Write-Host "Install Library Failed" -ForegroundColor Red
                        }
                        return $installlibraryresponse
                }
                else {
                        Write-Host "Library is already installed"
                }
        }
        else {
                Write-Host "cluster does not exist"
        }
}

function remove-ClusterJarLibrary($clustername, $jarpath)
{
        $clustersjson = get-DatabricksClusters
        $clusters = ConvertFrom-Json $clustersjson
        $clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id").cluster_id
        if($clusterid)
        {
                $installedlibraries = get-ClusterLibraryStatus $clusterid
                $found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library | Where-Object {$_.jar -eq $jarpath}
                if($found)
                {
                        $body = @{}
                        $body.cluster_id = $clusterid
                        $libraries = @{}
                        $libraries.jar = $jarpath
                        $body.libraries = $libraries
                        $bodyjson = $body | ConvertTo-Json
                        $bodyjson

                        $installlibraryuri = "$uribase/libraries/uninstall"
                        $installlibraryresponse = Invoke-WebRequest -Uri $installlibraryuri `
                                -Method Post `
                                -Authentication Bearer `
                                -Token $token `
                                -ContentType "application/json" `
                                -Body $bodyjson

                        if ($installlibraryresponse.StatusCode -ne 200)
                        {
                                Write-Host "Install Library Failed" -ForegroundColor Red
                        }
                        return $installlibraryresponse
                }
                else{
                        Write-Host "library not installed"
                }
        }
        else {
                Write-Host "cluster does not exist"
        }
}

function new-NEVADADatabricksJob ($jobname, $notebookpath, $datalakerelativepath, $datapackage, $adxdatabasename, $instancepoolid)
{
        $jobdatajson = get-DatabricksJobs
        $jobs = convertFrom-Json $jobdatajson.Content
        $jobdata = $jobs.jobs | Where-Object {$_.settings.name -eq $jobname} | Select-Object "job_id" | ConvertTo-Json
        if(!$jobdata)
        {
                $createjoburi = "$uribase/jobs/create"
                $jobinfo = @{}
                $newclusterinfo = @{}
                $jobemailinfo = @{}
                $jobscheduleinfo = @{}
                $notebooktaskinfo = @{}

                $spark_conf = @{}
                $spark_conf."spark.scheduler.mode" = "FAIR"

                #if($initscript -ne "")
                #{
                #        $initscriptinfo = @{}
                #        $dbfsstorageinfo = @{}
                #        $dbfsstorageinfo.destination = $initScript
                #        $initscriptinfo.dbfs = $dbfsstorageinfo
                        #$newclusterinfo.init_scripts = $initscriptinfo
                #}

                $newclusterinfo.num_workers = $max_workers
                $newclusterinfo.spark_version = $sparkversion
                $newclusterinfo.spark_conf = $spark_conf
                #$newclusterinfo.node_type_id = $nodetype
                $newclusterinfo.instance_pool_id = $instancepoolid

                $notebooktaskinfo.notebook_path = $notebookpath
                $baseparameterskvpair = @{}
                $baseparameterskvpair.relativePath = $datalakerelativepath
                $baseparameterskvpair.packageName = $datapackage
                $baseparameterskvpair.adxDatabaseName = $adxdatabasename
                $notebooktaskinfo.base_parameters = $baseparameterskvpair

                $jobemailinfo.on_failure = $emailnotification #also on_start, on_success

                if($cronschedule -ne "")
                {
                        $jobscheduleinfo.quartz_cron_expression = $cronschedule
                        $jobscheduleinfo.timezone_id = $timezoneid
                        $jobinfo.schedule = $jobscheduleinfo
                }

                $azmgmt = @{}
                $maven = @{}
                $coordinates = "com.microsoft.azure.kusto.v2019_09_07:azure-mgmt-kusto:1.0.0-beta"
                $maven.coordinates = $coordinates
                $azmgmt.maven = $maven

                $kustoingest = @{}
                $maven = @{}
                $coordinates = "com.microsoft.azure.kusto:kusto-ingest:1.2.0"
                $maven.coordinates = $coordinates
                $kustoingest.maven = $maven

                $kustodata = @{}
                $maven = @{}
                $coordinates = "com.microsoft.azure.kusto:kusto-data:1.2.0"
                $maven.coordinates = $coordinates
                $kustodata.maven = $maven

                $kustospark = @{}
                $maven = @{}
                $coordinates = "com.microsoft.azure.kusto:spark-kusto-connector:1.0.1"
                $maven.coordinates = $coordinates
                $kustospark.maven = $maven

                $libraries = @($azmgmt, $kustoingest, $kustodata, $kustospark)

                $jobinfo.new_cluster = $newclusterinfo
                $jobinfo.notebook_task = $notebooktaskinfo
                $jobinfo.name = $jobname
                $jobinfo.email_notifications = $jobemailinfo
                #$jobinfo.timeout_seconds = 6000
                $jobinfo.max_retries = 0
                $jobinfo.max_concurrent_runs = 1
                $jobinfo.libraries = $libraries
                $jobjson = $jobinfo | ConvertTo-Json -Depth 32
                $jobjson
                $createjobresponse = Invoke-WebRequest -Uri $createjoburi `
                        -Method Post `
                       -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $jobjson

                if ($createjobresponse.StatusCode -ne 200)
                {
                        Write-Host "Create Job Failed" -ForegroundColor Red
                }
                return $createjobresponse
        } else {
                Write-Host "job already exists"
        }
}

function new-InstancePool ($name, $minidle, $maxcapacity, $nodetypeid, $idleminutes, $sparkversion)
{
        $poolsjson = get-InstancePools
        $pools = ($poolsjson.Content | ConvertFrom-Json).instance_pools
        $pool = $pools | Where-Object {$_.instance_pool_name -eq $name} | Select-Object "instance_pool_name"
        if(!$pool)
        {
                $createpooluri = "$uribase/instance-pools/create"
                $poolinfo = @{}
                $poolinfo.instance_pool_name = $name
                $poolinfo.min_idle_instances = $minidle
                $poolinfo.max_capacity = $maxcapacity
                $poolinfo.node_type_id = $nodetypeid
                $poolinfo.idle_instance_autotermination_minutes = $idleminutes
                $preloadedsparkversions = @($sparkversion)
                $poolinfo.preloaded_spark_versions = $preloadedsparkversions

                $pooljson = $poolinfo | ConvertTo-Json -Depth 32
                $pooljson
                $createpoolresponse = Invoke-WebRequest -Uri $createpooluri `
                        -Method Post `
                       -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $pooljson

                if ($createpoolresponse.StatusCode -ne 200)
                {
                        Write-Host "Create Job Failed" -ForegroundColor Red
                }
                return $createpoolresponse
        } else {
                Write-Host "pool already exists"
        }
}

function remove-InstancePool ($name)
{
        $poolsjson = get-InstancePools
        $pools = ($poolsjson.Content | ConvertFrom-Json).instance_pools
        $instance_pool_id = $pools | Where-Object {$_.instance_pool_name -eq $name} | Select-Object "instance_pool_id"
        if($instance_pool_id.instance_pool_id)
        {
                $deletepooluri = "$uribase/instance-pools/delete"
                $poolinfo = @{}
                $poolinfo.instance_pool_id = $instance_pool_id.instance_pool_id

                $pooljson = $poolinfo | ConvertTo-Json -Depth 32
                $pooljson
                $deletepoolresponse = Invoke-WebRequest -Uri $deletepooluri `
                        -Method Post `
                       -Authentication Bearer `
                        -Token $token `
                        -ContentType "application/json" `
                        -Body $pooljson

                if ($deletepoolresponse.StatusCode -ne 200)
                {
                        Write-Host "Delete Pool Failed" -ForegroundColor Red
                }
                return $deletepoolresponse
        } else {
                Write-Host "pool does not exist"
        }
}

function get-InstancePools
{
        $uri = "$uribase/instance-pools/list"
        $response = Invoke-WebRequest -Uri $uri `
                -Method Get `
                -Authentication Bearer `
                -Token $token `
                -ContentType "application/json"
        if ($response.StatusCode -ne 200)
        {
                Write-Host "List Instance Pools Failed" -ForegroundColor Red
        }
        return $response
}
