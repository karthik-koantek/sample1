. $PSScriptRoot\Helper.ps1

Function Set-DatabricksSecretACLForPrincipal {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Principal,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$Permission
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.principal = $Principal
	$bodyHash.permission = $Permission
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/acls/put" -Token $Token -Body $body
}

Function Get-DatabricksSecretACLForPrincipal {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Principal
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.principal = $Principal
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/acls/get" -Token $Token -Body $body
}

Function Get-DatabricksSecretACLs {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/acls/list" -Token $Token -Body $body
}

Function Get-DatabricksSecretScopes {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/scopes/list" -Token $Token
}

Function New-DatabricksSecretScope {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ScopeName,
		[Parameter(Mandatory=$false, Position = 3)]
		[string]$InitialManagePrincipal = "users"
	)

	$scopeInfo = @{}
	$scopeInfo.scope = [string]$ScopeName
	if($InitialManagePrincipal -eq "users") {
		$scopeInfo.initial_manage_principal = [string]"users"
	}
	$scopeInfojson = New-Object -TypeName PSObject -Property $scopeInfo | ConvertTo-Json

	return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/scopes/create" -Token $Token -Body $scopeInfojson
}

Function Add-DatabricksSecretScope {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ScopeName,
		[Parameter(Mandatory=$false, Position = 3)]
		[string]$InitialManagePrincipal = "users"
	)

	$scopes = Get-DatabricksSecretScopes -BaseURI $BaseURI -Token $Token
	$scopeNames = ($scopes.Content | ConvertFrom-Json).scopes.name

	if($ScopeName -notin $scopeNames)
	{
		New-DatabricksSecretScope -BaseURI $BaseURI -Token $Token -ScopeName $ScopeName -InitialManagePrincipal $InitialManagePrincipal
	}
}

Function Remove-DatabricksSecretScope {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/scopes/delete" -Token $Token -Body $body
}

Function Remove-DatabricksSecret {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Key
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.key = $Key
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	$found = ((Get-DatabricksSecrets -BaseURI $URIBase -Token $Token -ScopeName $Scope).Content | ConvertFrom-Json).secrets | Where-Object {$_.key -eq $Key}
	if($found) {
		return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/delete" -Token $Token -Body $body
	} else {
		Write-Host "Secret does not exist."
	}
}

Function Get-DatabricksSecrets {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$ScopeName
	)

	$secretsInfo = @{}
	$secretsInfo.scope = [string]$ScopeName
	$scopeInfoJson = New-Object -TypeName PSObject -Property $secretsInfo | ConvertTo-Json

	return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/list" -Token $Token -Body $scopeInfoJson
}

Function Add-DatabricksSecret {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$StringValue,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$SecretScope,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$SecretKey
	)

	$secretInfo = @{}
	$secretInfo.string_value = [string]$StringValue
	$secretInfo.scope = [string]$SecretScope
	$secretInfo.key = [string]$SecretKey
	$secretInfojson = New-Object -TypeName PSObject -Property $secretInfo | ConvertTo-Json

	return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/put" -Token $Token -Body $secretInfojson
}

Function Set-DatabricksSecretACLForPrincipal {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Principal,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$Permission
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.principal = $Principal
	$bodyHash.permission = $Permission
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/acls/put" -Token $Token -Body $body
}

Function Get-DatabricksSecretACLForPrincipal {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Principal
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.principal = $Principal
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/acls/get" -Token $Token -Body $body
}

Function Remove-DatabricksSecretACLForPrincipal {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$Principal
	)

	$bodyHash = @{}
	$bodyHash.scope = $Scope
	$bodyHash.principal = $Principal
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	$found = ((Get-DatabricksSecretACLs -BaseURI $URIBase -Token $Token -Scope "internal").Content | ConvertFrom-Json).items | Where-Object {$_.principal -eq $Principal}

	if($found) {
		Return Invoke-PostRESTRequest -URI "${BaseURI}/secrets/acls/delete" -Token $Token -Body $body
	} else {
		Write-Host "Secret ACL does not exist"
	}
}

Function Get-DatabricksSecretACLs {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Scope
	)

	$bodyHash = @{}
	$bodyHash.scope = $scope
	$bodyObject = New-Object -TypeName PSObject -Property $bodyHash
	$body = $bodyObject | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/secrets/acls/list" -Token $Token -Body $body
}