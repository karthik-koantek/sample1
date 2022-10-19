. $PSScriptRoot\Helper.ps1

Function Get-DatabricksRepos {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$false, Position = 2)]
		[string]$PathPrefix = "/Repos",
		[Parameter(Mandatory=$false, Position = 3)]
		[string]$NextPageToken = ""
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/repos?path_prefix=${PathPrefix}" -Token $Token  
}

Function Get-DatabricksRepo {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$RepoId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/repos/${RepoId}" -Token $Token 
}

Function New-DatabricksRepo {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$URL,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Provider, #gitHub, bitbucketCloud, gitLab, azureDevOpsServices, gitHubEnterprise, bitbucketServer, gitLabEnterpriseEdition
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$Path
	)
	$RepoId = ((Get-DatabricksRepos -BaseURI $URIBase -Token $Token -PathPrefix $Path).Content | ConvertFrom-Json).repos.id
	if(!$RepoId) {
		$repoInfo = @{}
		$repoInfo.url = $URL 
		$repoInfo.provider = $Provider 
		$repoInfo.path = $Path 
		$repoJson = $repoInfo | ConvertTo-Json
		Return Invoke-PostRESTRequest -URI "${BaseURI}/repos" -Token $Token -Body $repoJson
	} else {
		Write-Host "Repo already exists."
	}
}

Function Remove-DatabricksRepo {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$RepoId
	)
	Return Invoke-DeleteRESTRequest -URI "${BaseURI}/repos/${RepoId}" -Token $Token 
}

Function Update-DatabricksRepo {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$RepoId,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Branch
	)
	$repoInfo = @{}
	$repoInfo.branch = $Branch 
	$repoJson = $repoInfo | ConvertTo-Json
	Return Invoke-PatchRESTRequest -URI "${BaseURI}/repos/${RepoId}" -Token $Token -Body $repoJson
}

