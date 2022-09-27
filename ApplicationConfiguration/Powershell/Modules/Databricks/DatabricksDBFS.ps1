. $PSScriptRoot\Helper.ps1

Function Import-LocalFileToDBFS {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Path,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$LocalFile,
		[Parameter(Position = 4)]
		[boolean]$Overwrite = $false
	)

	$base64string = [Convert]::ToBase64String([IO.File]::ReadAllBytes($LocalFile))
	$body = @{}
	$body.path = $Path
	$body.contents = $base64string
	$body.overwrite = $Overwrite
	$bodyjson = New-Object -TypeName PSObject -Property $body | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/dbfs/put" -Token $Token -Body $bodyjson
}
