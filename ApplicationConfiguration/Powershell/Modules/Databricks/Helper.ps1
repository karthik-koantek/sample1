Function Invoke-GetRESTRequest {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$URI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Position = 2)]
		[object]$Body = $null,
		[Parameter(Position = 3)]
		[string]$ContentType = "application/json",
		[Parameter(Position = 4)]
		[int32]$ExpectedStatusCode = 200,
		[Parameter(Position = 5)]
		[string]$OutFile = $null
	)

	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

	$response = Invoke-WebRequest `
		-Uri $URI `
		-Method Get `
		-Authentication Bearer `
		-Token $Token `
		-ContentType $ContentType `
		-Body $Body `
		-OutFile $OutFile

	if ([string]::IsNullOrEmpty($OutFile)) {
		If($response.StatusCode -ne $ExpectedStatusCode) {
			throw "Invoke-GetRESTRequest failed"
		}
	}

	Return $response
}

Function Invoke-DeleteRESTRequest {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$URI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Position = 2)]
		[int32]$ExpectedStatusCode = 200,
		[Parameter(Position = 3)]
		[string]$ContentType = "application/json"
	)

	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

	$response = Invoke-WebRequest `
		-Uri $URI `
		-Method Delete `
		-Authentication Bearer `
		-Token $Token `
		-ContentType $ContentType

	If($response.StatusCode -ne $ExpectedStatusCode) {
		throw "Invoke-DeleteRESTRequest failed"
	}

	Return $response
}

Function Invoke-PostRESTRequest {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$URI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Position = 2)]
		[object]$Body = $null,
		[Parameter(Position = 3)]
		[string]$OutFile = $null,
		[Parameter(Position = 4)]
		[int32]$ExpectedStatusCode = 200,
		[Parameter(Position = 5)]
		[string]$ContentType = "application/json"
	)

	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

	$response = Invoke-WebRequest `
		-Uri $URI `
		-Method Post `
		-Authentication Bearer `
		-Token $Token `
		-ContentType $ContentType `
		-Body $Body `
		-OutFile $OutFile

	If($response.StatusCode -ne $ExpectedStatusCode) {
		throw "Invoke-PostRESTRequest failed"
	}

	Return $response
}

Function Invoke-PutRESTRequest {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$URI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Position = 2)]
		[object]$Body = $null,
		[Parameter(Position = 3)]
		[string]$OutFile = $null,
		[Parameter(Position = 4)]
		[int32]$ExpectedStatusCode = 200,
		[Parameter(Position = 5)]
		[string]$ContentType = "application/json"
	)

	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

	$response = Invoke-WebRequest `
		-Uri $URI `
		-Method Put `
		-Authentication Bearer `
		-Token $Token `
		-ContentType $ContentType `
		-Body $Body `
		-OutFile $OutFile

	If($response.StatusCode -ne $ExpectedStatusCode) {
		throw "Invoke-PutRESTRequest failed"
	}

	Return $response
}

Function Invoke-PatchRESTRequest {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$URI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Position = 2)]
		[object]$Body = $null,
		[Parameter(Position = 3)]
		[string]$OutFile = $null,
		[Parameter(Position = 4)]
		[int32]$ExpectedStatusCode = 200,
		[Parameter(Position = 5)]
		[string]$ContentType = "application/json"
	)

	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12

	$response = Invoke-WebRequest `
		-Uri $URI `
		-Method Patch `
		-Authentication Bearer `
		-Token $Token `
		-ContentType $ContentType `
		-Body $Body `
		-OutFile $OutFile

	If($response.StatusCode -ne $ExpectedStatusCode) {
		throw "Invoke-PatchRESTRequest failed"
	}

	Return $response
}

Function Get-AADBearerToken {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$TenantId,
		[Parameter(Mandatory=$true, Position = 1)]
		[string]$ClientId,
		[Parameter(Mandatory=$false, Position = 2)]
		[string]$Resource = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d",
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$ClientSecret
	)

	$URI = "https://login.microsoftonline.com/${TenantId}/oauth2/token"
	$grant_type="client_credentials"
	$ContentType = "application/x-www-form-urlencoded"
	[Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
	$Body = "grant_type=${grant_type}&client_id=${ClientId}&resource=${Resource}&client_secret=${ClientSecret}"
	$response = curl -X POST -H "Content-Type: $ContentType" -d $Body $URI
	$AADToken = ($response | ConvertFrom-Json).access_token
	$AADSecureToken = ConvertTo-SecureString -String $AADToken -AsPlainText -Force
	Return $AADSecureToken
}