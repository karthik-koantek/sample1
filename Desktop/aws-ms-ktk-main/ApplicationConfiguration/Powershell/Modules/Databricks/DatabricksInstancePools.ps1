. $PSScriptRoot\Helper.ps1

Function New-InstancePool {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Name,
		[Parameter(Mandatory=$true, Position = 3)]
		[int]$MinIdle,
		[Parameter(Mandatory=$true, Position = 4)]
		[int]$MaxCapacity,
		[Parameter(Mandatory=$true, Position = 5)]
		[string]$NodeTypeID,
		[Parameter(Mandatory=$true, Position = 6)]
		[int]$IdleMinutes,
		[Parameter(Mandatory=$true, Position = 7)]
		[string]$SparkVersion,
		[Parameter(Mandatory=$false, Position = 8)]
		[string]$AzureAvailability = "SPOT_WITH_FALLBACK_AZURE", #SPOT_AZURE, ON_DEMAND_AZURE, SPOT_WITH_FALLBACK_AZURE
		[Parameter(Mandatory=$false, Position = 9)]
		[double]$SpotBidMaxPrice = -1
	)

	$poolsjson = Get-InstancePools -BaseURI $BaseURI -Token $Token
	$pools = ($poolsjson.Content | ConvertFrom-Json).instance_pools
	$pool = $pools | Where-Object {$_.instance_pool_name -eq $Name} | Select-Object "instance_pool_name"
	If(!$pool) {
		$poolinfo = @{}
		$poolinfo.instance_pool_name = $Name
		$poolinfo.min_idle_instances = $MinIdle
		$poolinfo.max_capacity = $MaxCapacity
		$poolinfo.node_type_id = $NodeTypeID
		$poolinfo.idle_instance_autotermination_minutes = $IdleMinutes
		$preloadedsparkversions = @($SparkVersion)
		$poolinfo.preloaded_spark_versions = $preloadedsparkversions

		$AzureAttributes = @{}
		$AzureAttributes.availability = $AzureAvailability
		$AzureAttributes.spot_bid_max_price = $SpotBidMaxPrice
		$poolinfo.azure_attributes = $AzureAttributes 

		$pooljson = $poolinfo | ConvertTo-Json -Depth 32

		Return Invoke-PostRESTRequest -URI "${BaseURI}/instance-pools/create" -Token $Token -Body $pooljson
	} Else {
		Write-Host "Instance pool already exists"
	}
}

Function Remove-InstancePool {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Name
	)

	$poolsjson = Get-InstancePools -BaseURI $BaseURI -Token $Token
	$pools = ($poolsjson.Content | ConvertFrom-Json).instance_pools
	$instance_pool_id = $pools | Where-Object {$_.instance_pool_name -eq $Name} | Select-Object "instance_pool_id"
	If($instance_pool_id.instance_pool_id) {
		$poolinfo = @{}
		$poolinfo.instance_pool_id = $instance_pool_id.instance_pool_id

		$pooljson = $poolinfo | ConvertTo-Json -Depth 32

		Return Invoke-PostRESTRequest -URI "${BaseURI}/instance-pools/delete" -Token $Token -Body $pooljson
	} Else {
		Write-Host "Instance pool does not exist"
	}
}

Function Get-InstancePools {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	return Invoke-GetRESTRequest -URI "${BaseURI}/instance-pools/list" -Token $Token
}

Function Get-InstancePool {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$InstancePoolName
	)

	Return (((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools | Where-Object {$_.instance_pool_name -eq $InstancePoolName}).instance_pool_id
}