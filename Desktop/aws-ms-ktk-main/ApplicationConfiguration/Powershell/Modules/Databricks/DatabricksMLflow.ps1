. $PSScriptRoot\Helper.ps1

#https://mlflow.org/docs/latest/rest-api.html
#https://docs.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/mlflow

Function Get-Experiments {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/mlflow/experiments/list" -Token $Token  
}

Function Get-Models {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/preview/mlflow/registered-models/list" -Token $Token  
}

Function Get-Model {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 1)]
		[string]$ModelName
    )
    $ModelInfo = @{}
    $ModelInfo.name = $ModelName 
    Return Invoke-GetRESTRequest -URI "${BaseURI}/preview/mlflow/registered-models/get" -Token $Token -Body $ModelInfo 
}