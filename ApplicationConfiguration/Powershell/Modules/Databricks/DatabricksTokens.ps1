. $PSScriptRoot\Helper.ps1

Function Get-DatabricksTokens {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/token/list" -Token $Token
}

Function Get-DatabricksToken {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Comment
	)
	$DatabricksTokens = ((Get-DatabricksTokens -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).token_infos
	$DatabricksToken = $DatabricksTokens | Where-Object {$_.comment -eq $Comment}
	return $DatabricksToken
}

Function New-DatabricksToken {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$false, Position = 2)]
		[string]$LifetimeSeconds = -1,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Comment
	)

	$DatabricksToken = Get-DatabricksToken -BaseURI $URIBase -Token $Token -Comment $Comment
	if(!$DatabricksToken) {
		$tokenInfo = @{}
		if($LifetimeSeconds -ne -1) {$tokenInfo.lifetime_seconds = $LifetimeSeconds}
		if($Comment -ne "") {$tokeninfo.comment = $Comment}
		$tokenInfojson = New-Object -TypeName PSObject -Property $tokenInfo | ConvertTo-Json

		Return Invoke-PostRESTRequest -URI "${BaseURI}/token/create" -Token $Token -Body $tokenInfojson
	} else {
		Write-Host "Token already exists."
	}
}

Function Revoke-DatabricksToken {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$false, Position = 2)]
		[string]$TokenIdToDelete
	)
	$DatabricksTokens = ((Get-DatabricksTokens -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).token_infos
	$DatabricksToken = $DatabricksTokens | Where-Object {$_.token_id -eq $TokenIdToDelete}
	if($DatabricksToken) {
		$tokenInfo = @{}
		$tokenInfo.token_id = $TokenIdToDelete
		$tokenInfojson = New-Object -TypeName PSObject -Property $tokenInfo | ConvertTo-Json
		Return Invoke-PostRESTRequest -URI "${BaseURI}/token/delete" -Token $Token -Body $tokenInfojson
	} else {
		Write-Host "Token does not exist."
	}
}