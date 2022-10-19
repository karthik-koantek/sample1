. $PSScriptRoot\Helper.ps1

Function Get-DatabricksClusters {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	return Invoke-GetRESTRequest -URI "${BaseURI}/clusters/list" -Token $Token
}

Function Remove-DatabricksCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id"

	If($clusterdata) {
		$clusterinfo = @{}
		$clusterinfo.cluster_id = $clusterdata.cluster_id
		$clusterjson = $clusterinfo | ConvertTo-Json

		return Invoke-PostRESTRequest -URI "${BaseURI}/clusters/permanent-delete" -Token $Token -Body $clusterjson
	} else {
		Write-Host "Cluster does not exist"
	}
}

Function New-DatabricksCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$false, Position = 3)]
		[int]$MinWorkers = 0,
		[Parameter(Mandatory=$true, Position = 4)]
		[int]$MaxWorkers,
		[Parameter(Mandatory=$true, Position = 5)]
		[int]$AutoTerminationMinutes,
		[Parameter(Mandatory=$true, Position = 6)]
		[string]$SparkVersion,
		[Parameter(Mandatory=$true, Position = 7)]
		[string]$NodeType,
		[Parameter(Mandatory=$true, Position = 8)]
		[string]$DriverNodeType,
		[Parameter(Mandatory=$false, Position = 9)]
		[string]$InitScriptPath = "",
		[Parameter(Mandatory=$false, Position = 10)]
		[string]$InstancePoolId = "",
		[Parameter(Mandatory=$false, Position = 11)]
		$StorageAccountHash = @{},
		[Parameter(Mandatory=$false, Position = 12)]
		[string]$AzureAvailability = "SPOT_WITH_FALLBACK_AZURE", #SPOT_AZURE, ON_DEMAND_AZURE, SPOT_WITH_FALLBACK_AZURE
		[Parameter(Mandatory=$false, Position = 13)]
		[double]$SpotBidMaxPrice = -1,
		[Parameter(Mandatory=$false, Position = 14)]
		[int]$FirstOnDemand = 1
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json

	If(!$clusterdata) {
		$clusterinfo = @{}
		if($MinWorkers -eq 0) {
			$clusterinfo.num_workers = $MaxWorkers
		} else {
			$autoscaleinfo = @{}
			$autoscaleinfo.min_workers = $MinWorkers
			$autoscaleinfo.max_workers = $MaxWorkers
			$autoscale = New-Object -TypeName PSObject -Property $autoscaleinfo
			$clusterinfo.autoscale = $autoscale
		}
		$spark_env_vars = @{}
		$spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"

		if($InitScriptPath -ne "") {
			$initscriptinfo = @{}
			$dbfsstorageinfo = @{}
			$dbfsstorageinfo.destination = $InitScriptPath
			$initscriptinfo.dbfs = $dbfsstorageinfo
			$clusterinfo.init_scripts = $initscriptinfo
		}

		$clusterinfo.cluster_name = $ClusterName
		$clusterinfo.spark_version = $SparkVersion
		if ($InstancePoolId -ne "") {
			$clusterinfo.instance_pool_id = $InstancePoolId
		} else {
			$clusterinfo.node_type_id = $NodeType
			$clusterinfo.driver_node_type_id = $DriverNodeType
		}

		$AzureAttributes = @{}
		$AzureAttributes.availability = $AzureAvailability
		$AzureAttributes.spot_bid_max_price = $SpotBidMaxPrice
		$AzureAttributes.first_on_demand = $FirstOnDemand
		$clusterinfo.azure_attributes = $AzureAttributes

		$spark_conf = @{}
		$spark_conf."spark.scheduler.mode" = "FAIR"
		$spark_conf."spark.sql.adaptive.enabled" = "true"
		if ($StorageAccountHash) {
			foreach ($sa in $StorageAccountHash.Keys) {
				Write-Host $sa, $StorageAccountHash.$sa
				$spark_conf.$sa = $StorageAccountHash.$sa
			}
		}
		$clusterinfo.spark_conf = $spark_conf
		$clusterinfo.spark_env_vars = $spark_env_vars
		$clusterinfo.autotermination_minutes = $AutoTerminationMinutes
		$clusterinfo = New-Object -TypeName psobject -Property $clusterinfo
		$clusterjson = $clusterinfo | ConvertTo-Json

		Return Invoke-PostRESTRequest -URI "${BaseURI}/clusters/create" -Token $Token -Body $clusterjson
	} Else {
		Write-Host "cluster already exists"
	}
}

Function New-DatabricksHighConcurrencyCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$false, Position = 3)]
		[int]$MinWorkers = 0,
		[Parameter(Mandatory=$true, Position = 4)]
		[int]$MaxWorkers,
		[Parameter(Mandatory=$true, Position = 5)]
		[int]$AutoTerminationMinutes,
		[Parameter(Mandatory=$true, Position = 6)]
		[string]$SparkVersion,
		[Parameter(Mandatory=$true, Position = 7)]
		[string]$NodeType,
		[Parameter(Mandatory=$true, Position = 8)]
		[string]$DriverNodeType,
		[Parameter(Mandatory=$false, Position = 9)]
		[string]$InitScriptPath = "",
		[Parameter(Mandatory=$false, Position = 10)]
		[string]$InstancePoolId = "",
		[Parameter(Mandatory=$false, Position = 11)]
		$StorageAccountHash = @{},
		[Parameter(Mandatory=$false, Position = 12)]
		[bool]$EnableTableAccessControl = $true,
		[Parameter(Mandatory=$false, Position = 13)]
		[bool]$EnableADLSCredentialPassthrough = $false,
		[Parameter(Mandatory=$false, Position = 14)]
		[string]$AzureAvailability = "SPOT_WITH_FALLBACK_AZURE", #SPOT_AZURE, ON_DEMAND_AZURE, SPOT_WITH_FALLBACK_AZURE
		[Parameter(Mandatory=$false, Position = 15)]
		[double]$SpotBidMaxPrice = -1,
		[Parameter(Mandatory=$false, Position = 16)]
		[int]$FirstOnDemand = 1
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json

	If(!$clusterdata) {
		$clusterinfo = @{}
		if($MinWorkers -eq 0) {
			$clusterinfo.num_workers = $MaxWorkers
		} else {
			$autoscaleinfo = @{}
			$autoscaleinfo.min_workers = $MinWorkers
			$autoscaleinfo.max_workers = $MaxWorkers
			$autoscale = New-Object -TypeName PSObject -Property $autoscaleinfo
			$clusterinfo.autoscale = $autoscale
		}
		$spark_env_vars = @{}
		$spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"

		if($InitScriptPath -ne "") {
			$initscriptinfo = @{}
			$dbfsstorageinfo = @{}
			$dbfsstorageinfo.destination = $InitScriptPath
			$initscriptinfo.dbfs = $dbfsstorageinfo
			$clusterinfo.init_scripts = $initscriptinfo
		}

		$clusterinfo.cluster_name = $ClusterName
		$clusterinfo.spark_version = $SparkVersion
		if ($InstancePoolId -ne "") {
			$clusterinfo.instance_pool_id = $InstancePoolId
		} else {
			$clusterinfo.node_type_id = $NodeType
			$clusterinfo.driver_node_type_id = $DriverNodeType
		}

		$AzureAttributes = @{}
		$AzureAttributes.availability = $AzureAvailability
		$AzureAttributes.spot_bid_max_price = $SpotBidMaxPrice
		$AzureAttributes.first_on_demand = $FirstOnDemand
		$clusterinfo.azure_attributes = $AzureAttributes

		$spark_conf = @{}
		$spark_conf."spark.databricks.cluster.profile" = "serverless"
		if($EnableADLSCredentialPassthrough -eq $true) {$spark_conf."spark.databricks.passthrough.enabled" = "true"}
		if($EnableTableAccessControl -eq $true) {$spark_conf."spark.databricks.acl.dfAclsEnabled" = "true"}
		$spark_conf."spark.databricks.pyspark.enableProcessIsolation" = "true"
		$spark_conf."spark.databricks.repl.allowedLanguages" = "python,sql"
		$spark_conf."spark.sql.adaptive.enabled" = "true"
		if ($StorageAccountHash) {
			foreach ($sa in $StorageAccountHash.Keys) {
				Write-Host $sa, $StorageAccountHash.$sa
				$spark_conf.$sa = $StorageAccountHash.$sa
			}
		}
		$clusterinfo.spark_conf = $spark_conf
		$clusterinfo.spark_env_vars = $spark_env_vars
		$clusterinfo.autotermination_minutes = $AutoTerminationMinutes
		$clusterinfo = New-Object -TypeName psobject -Property $clusterinfo
		$clusterjson = $clusterinfo | ConvertTo-Json

		Return Invoke-PostRESTRequest -URI "${BaseURI}/clusters/create" -Token $Token -Body $clusterjson
	} Else {
		Write-Host "cluster already exists"
	}
}

Function New-DatabricksSingleNodeCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[int]$AutoTerminationMinutes,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$SparkVersion,
		[Parameter(Mandatory=$true, Position = 5)]
		[string]$NodeType,
		[Parameter(Mandatory=$false, Position = 6)]
		[string]$InitScriptPath = "",
		[Parameter(Mandatory=$false, Position = 7)]
		[string]$InstancePoolId = "",
		[Parameter(Mandatory=$false, Position = 8)]
		$StorageAccountHash = @{},
		[Parameter(Mandatory=$false, Position = 9)]
		[string]$AzureAvailability = "SPOT_WITH_FALLBACK_AZURE", #SPOT_AZURE, ON_DEMAND_AZURE, SPOT_WITH_FALLBACK_AZURE
		[Parameter(Mandatory=$false, Position = 10)]
		[double]$SpotBidMaxPrice = -1,
		[Parameter(Mandatory=$false, Position = 11)]
		[int]$FirstOnDemand = 1
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json

	If(!$clusterdata) {
		$clusterinfo = @{}
		$clusterinfo.num_workers = 0
		$spark_env_vars = @{}
		$spark_env_vars.PYSPARK_PYTHON = "/databricks/python3/bin/python3"

		if($InitScriptPath -ne "") {
			$initscriptinfo = @{}
			$dbfsstorageinfo = @{}
			$dbfsstorageinfo.destination = $InitScriptPath
			$initscriptinfo.dbfs = $dbfsstorageinfo
			$clusterinfo.init_scripts = $initscriptinfo
		}

		$clusterinfo.cluster_name = $ClusterName
		$clusterinfo.spark_version = $SparkVersion
		if ($InstancePoolId -ne "") {
			$clusterinfo.instance_pool_id = $InstancePoolId
		} else {
			$clusterinfo.node_type_id = $NodeType
			$clusterinfo.driver_node_type_id = $NodeType
		}

		$spark_conf = @{}
		$spark_conf."spark.databricks.cluster.profile" = "singleNode"
		$spark_conf."spark.master" = "local[*]"
		$spark_conf."spark.databricks.delta.preview.enabled" = "true"
		if ($StorageAccountHash) {
			foreach ($sa in $StorageAccountHash.Keys) {
				Write-Host $sa, $StorageAccountHash.$sa
				$spark_conf.$sa = $StorageAccountHash.$sa
			}
		}
		$clusterinfo.spark_conf = $spark_conf
		$clusterinfo.spark_env_vars = $spark_env_vars
		$clusterinfo.autotermination_minutes = $AutoTerminationMinutes
		$clusterinfo = New-Object -TypeName psobject -Property $clusterinfo
		$clusterjson = $clusterinfo | ConvertTo-Json

		Return Invoke-PostRESTRequest -URI "${BaseURI}/clusters/create" -Token $Token -Body $clusterjson
	} Else {
		Write-Host "cluster already exists"
	}
}

Function Start-DatabricksCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json

	If($clusterdata) {
		If($clusters.clusters.state -eq "TERMINATED") {
			Return Invoke-PostRESTRequest -URI "${BaseURI}/clusters/start" -Token $Token -Body $clusterdata
		} Else {
				Write-Host "Cluster is running"
		}
	} Else {
		Write-Host "Cluster not found"
	}
}

Function Start-DatabricksClusterAwaitUntilStarted {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)
	Do {
        $ClustersJSON = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
        $Clusters = ConvertFrom-Json $ClustersJSON
        $ClusterData = $Clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id", "state"
        $ClusterData
        If($ClusterData.state -eq "TERMINATED") {
            Write-Host "Starting Cluster $ClusterName" -ForegroundColor Cyan
            Start-DatabricksCluster -BaseURI $BaseURI -Token $Token -ClusterName $ClusterName
        }
        If(($ClusterData.state -eq "TERMINATED") -or ($ClusterData.state -eq "PENDING")) {
            Start-Sleep 60
        }
    } While(($ClusterData.state -eq "TERMINATED") -or ($clusterdata.state -eq "PENDING"))
	Return $ClusterData
}

Function Stop-DatabricksCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Position = 3)]
		[boolean]$Permanent = $false
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $clustername} | Select-Object "cluster_id" | ConvertTo-Json

	If($Permanent -eq $false) {
		$deleteclusteruri = "${BaseURI}/clusters/delete"
	} Else {
		$deleteclusteruri = "${BaseURI}/clusters/permanent-delete"
	}

	Return Invoke-PostRESTRequest -URI $deleteclusteruri -Token $Token -Body $clusterdata
}

Function Get-DatabricksCluster {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json
	$cluster_id = ($clusterdata | ConvertFrom-Json).cluster_id
	Return $cluster_id
}

Function Get-DatabricksClusterDetail {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterdata = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id" | ConvertTo-Json
	$cluster_id = ($clusterdata | ConvertFrom-Json).cluster_id

	Return Invoke-GetRESTRequest -URI "${BaseURI}/clusters/get?cluster_id=${cluster_id}" -Token $Token
}

Function Stop-RunningDatabricksClusters {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = $clustersjson | ConvertFrom-Json
	$clusters = $clusters.clusters
	foreach ($cluster in $clusters) {
		If($cluster.state -ne "TERMINATED") {
			Stop-DatabricksCluster -BaseURI $BaseURI -Token $Token -ClusterName $cluster.cluster_name
		}
	}
}