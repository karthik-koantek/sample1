. $PSScriptRoot\Helper.ps1
. $PSScriptRoot\DatabricksClusters.ps1

Function Get-ClusterLibraryStatuses {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	return Invoke-GetRESTRequest -URI "${BaseURI}/libraries/all-cluster-statuses" -Token $Token
}

Function Get-ClusterLibraryStatus {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterID
	)

	return Invoke-GetRESTRequest -URI "${BaseURI}/libraries/cluster-status?cluster_id=${ClusterID}" -Token $Token
}

Function Remove-ClusterMavenLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$MavenCoordinate
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.maven | Where-Object {$_.coordinates -eq $MavenCoordinate}
		if($found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$maven = @{}
			$maven.coordinates = $MavenCoordinate
			$libraries.maven = $maven
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json

			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/uninstall" -Token $Token -Body $bodyjson
		} else{
			Write-Host "Library not installed"
		}
	} else {
		Write-Host "Cluster does not exist"
	}
}

Function Remove-ClusterPyPiLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$PyPiPackage
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.pypi | Where-Object {$_.package -eq $PyPiPackage}
		if($found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$pypi = @{}
			$pypi.package = $PyPiPackage
			$libraries.pypi = $pypi
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json

			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/uninstall" -Token $Token -Body $bodyjson
		} else{
			Write-Host "Library not installed"
		}
	} else {
		Write-Host "Cluster does not exist"
	}
}

Function New-ClusterMavenLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$MavenCoordinate
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -BaseURI $BaseURI -Token $Token -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.maven | Where-Object {$_.coordinates -eq $MavenCoordinate}
		if(!$found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$maven = @{}
			$maven.coordinates = $MavenCoordinate
			$libraries.maven = $maven
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json

			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/install" -Token $Token -Body $bodyjson
		} else {
			Write-Host "Library is already installed"
		}
	} else {
		Write-Host "Cluster does not exist"
	}
}

Function New-ClusterPyPiLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$PyPiPackage
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -BaseURI $BaseURI -Token $Token -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library.pypi | Where-Object {$_.package -eq $PyPiPackage}
		if(!$found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$pypi = @{}
			$pypi.package = $PyPiPackage
			$libraries.pypi = $pypi
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json

			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/install" -Token $Token -Body $bodyjson
		} else {
			Write-Host "Library is already installed"
		}
	} else {
		Write-Host "Cluster does not exist"
	}
}

Function New-ClusterJarLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$JarPath
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -BaseURI $BaseURI -Token $Token -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library | Where-Object {$_.jar -eq $JarPath}
		if(!$found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$jar = $JarPath
			$libraries.jar = $jar
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json

			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/install" -Token $Token -Body $bodyjson
		} else {
			Write-Host "Library is already installed"
		}
	} Else {
		Write-Host "Cluster does not exist"
	}
}

Function Remove-ClusterJarLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$JarPath
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = ($clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id").cluster_id
	if($clusterid)
	{
		$installedlibraries = Get-ClusterLibraryStatus -BaseURI $BaseURI -Token $Token -ClusterID $clusterid
		$found = ($installedlibraries.Content | ConvertFrom-Json).library_statuses.library | Where-Object {$_.jar -eq $JarPath}
		if($found)
		{
			$body = @{}
			$body.cluster_id = $clusterid
			$libraries = @{}
			$libraries.jar = $JarPath
			$body.libraries = $libraries
			$bodyjson = $body | ConvertTo-Json
			Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/uninstall" -Token $Token -Body $bodyjson
		} else{
			Write-Host "Library not installed"
		}
	} Else {
		Write-Host "cluster does not exist"
	}
}

Function New-ClusterCustomLibrary {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$ClusterName,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$LibraryType
	)

	$clustersjson = Get-DatabricksClusters -BaseURI $BaseURI -Token $Token
	$clusters = ConvertFrom-Json $clustersjson
	$clusterid = $clusters.clusters | Where-Object {$_.cluster_name -eq $ClusterName} | Select-Object "cluster_id"
	if($clusterid)
	{
		$body = @{"cluster_id" = $clusterid.cluster_id}
		$Libraries = @()
		$Library = @{}
		$Library[$LibraryType] = $Path
		$Libraries += $Library
		$body['libraries'] = $Libraries
		$bodyjson = $body | ConvertTo-Json -Depth 100
		Return Invoke-PostRESTRequest -URI "${BaseURI}/libraries/install" -Token $Token -Body $bodyjson
	} else {
		Write-Host "cluster does not exist"
	}
}