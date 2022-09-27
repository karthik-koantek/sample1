. $PSScriptRoot\Helper.ps1

Function Get-SQLEndpoints {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/sql/endpoints" -Token $Token
}

Function Get-SQLEndpoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndPointName
    )
    Return ((Get-SQLEndpoints -BaseURI $BaseURI -Token $Token).Content | ConvertFrom-Json).endpoints | Where-Object {$_.name -eq $SQLEndPointName}
}

Function Start-SQLEndpoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndPointName
    )
    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $SQLEndPointName).id
    if($EndpointId) {
        Return Invoke-PostRESTRequest -URI "${BaseURI}/sql/endpoints/${EndpointId}/start" -Token $Token
    }
}

Function Stop-SQLEndpoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndPointName
    )
    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $SQLEndPointName).id
    if($EndpointId) {
        Return Invoke-PostRESTRequest -URI "${BaseURI}/sql/endpoints/${EndpointId}/stop" -Token $Token
    }
}

Function Remove-SQLEndpoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndPointName
    )
    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $SQLEndPointName).id
    if($EndpointId) {
        Return Invoke-DeleteRESTRequest -URI "${BaseURI}/sql/endpoints/${EndpointId}" -Token $Token
    }
}

Function New-EndpointConfPair {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$Key,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$Value
    )
    $EndpointConfPair = @{}
    $EndpointConfPair.key = $Key 
    $EndpointConfPair.value = $Value 
    Return $EndpointConfPair 
}

Function Get-GlobalSQLEndpointConfiguration {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
    )
    Return Invoke-GetRESTRequest -URI "${BaseURI}/sql/config/endpoints" -Token $Token
}

Function Edit-GlobalSQLEndpointConfiguration {

    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$false, Position = 2)]
        [string]$EndpointSecurityPolicy = "DATA_ACCESS_CONTROL",
        [Parameter(Mandatory=$false, Position = 3)]
        [bool]$UseServicePrincipalAuth = $true,
        [Parameter(Mandatory=$false, Position = 3)]
        $StorageAccountHash = @{},
        [Parameter(Mandatory=$false, Position = 4)]
        [string]$ApplicationId = "{{secrets/internal/ClientId}}",
        [Parameter(Mandatory=$false, Position = 5)]
        [string]$ApplicationSecret = "{{secrets/internal/ClientSecret}}",
        [Parameter(Mandatory=$false, Position = 6)]
        [string]$TenantId = ""
    )
    $EndpointInfo = @{}
    $EndpointInfo.security_policy = $EndpointSecurityPolicy
    [System.Collections.ArrayList]$DataAccessConfig = @()
    foreach ($StorageAccountKey in $StorageAccountHash.keys) {
        if ($UseServicePrincipalAuth -eq $false) {
            #Internal Storage Account Keys
            $dfs = New-EndpointConfPair -key $StorageAccountKey -value $StorageAccountHash.$StorageAccountKey 
            $DataAccessConfig.add($dfs) | Out-Null 
        } else {
            #Service Principal
            if ($TenantId -ne "") {
                $StorageAccount = $StorageAccountKey.replace("fs.azure.account.key.","")
                $OAuth = New-EndpointConfPair -key "spark.hadoop.fs.azure.account.auth.type.$StorageAccount" -value "OAuth"
                $DataAccessConfig.add($OAuth) | Out-Null
                $Provider = New-EndpointConfPair -key "spark.hadoop.fs.azure.account.oauth.provider.type.$StorageAccount" -value "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider" 
                $DataAccessConfig.add($Provider) | Out-Null
                $Client = New-EndpointConfPair -key "spark.hadoop.fs.azure.account.oauth2.client.id.$StorageAccount" -value $ApplicationId
                $DataAccessConfig.add($Client) | Out-Null
                $Secret = New-EndpointConfPair -key "spark.hadoop.fs.azure.account.oauth2.client.secret.$StorageAccount" -value $ApplicationSecret 
                $DataAccessConfig.add($Secret) | Out-Null
                $ClientEndpoint = New-EndPOintConfPair -key "spark.hadoop.fs.azure.account.oauth2.client.endpoint.$StorageAccount" -value "https://login.microsoftonline.com/$TenantId/oauth2/token"
                $DataAccessConfig.add($ClientEndpoint) | Out-Null
            }
        }
    }
    $EndpointInfo.data_access_config = $DataAccessConfig 
    $EndpointInfoJSON = $EndpointInfo | ConvertTo-Json -Depth 32
    Return Invoke-PutRESTRequest -URI "${BaseURI}/sql/config/endpoints" -Token $Token -Body $EndpointInfoJSON 
}

Function New-SQLEndPoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EndpointName,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$ClusterSize, #2x-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$MinNumClusters=1,
        [Parameter(Mandatory=$true, Position = 5)]
        [int]$MaxNumClusters,
        [Parameter(Mandatory=$true, Position = 6)]
        [int]$AutoStopMinutes,
        [Parameter(Mandatory=$false, Position = 7)]
        $tags = @{},
        [Parameter(Mandatory=$false, Position = 8)]
        [bool]$EnablePhoton = $true
    )

    $EndpointInfo = @{}
    $EndpointInfo.name = $EndpointName 
    $EndpointInfo.cluster_size = $ClusterSize 
    $EndpointInfo.min_num_clusters = $MinNumClusters
    $EndpointInfo.max_num_clusters = $MaxNumClusters 
    $EndpointInfo.auto_stop_mins = $AutoStopMinutes
    $EndpointInfo.tags = $tags 
    $EndpointInfo.enable_photon = $EnablePhoton
    $EndpointInfoJSON = $EndpointInfo | ConvertTo-Json -Depth 32
    
    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $EndPointName).id
    if(!$EndpointId) {
        Return Invoke-PostRESTRequest -URI "${BaseURI}/sql/endpoints" -Token $Token -Body $EndpointInfoJSON
    } else {
        Write-Host "Endpoint already exists."
    }
}

Function Edit-SQLEndPoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EndpointName,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$ClusterSize, #2x-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$MinNumClusters=1,
        [Parameter(Mandatory=$true, Position = 5)]
        [int]$MaxNumClusters,
        [Parameter(Mandatory=$true, Position = 6)]
        [int]$AutoStopMinutes,
        [Parameter(Mandatory=$false, Position = 7)]
        $tags = @{},
        [Parameter(Mandatory=$false, Position = 8)]
        [bool]$EnablePhoton = $true
    )

    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $EndPointName).id
    $EndpointInfo = @{}
    $EndpointInfo.id = $EndpointId
    $EndpointInfo.name = $EndpointName 
    $EndpointInfo.cluster_size = $ClusterSize 
    $EndpointInfo.min_num_clusters = $MinNumClusters
    $EndpointInfo.max_num_clusters = $MaxNumClusters 
    $EndpointInfo.auto_stop_mins = $AutoStopMinutes
    $EndpointInfo.tags = $tags 
    $EndpointInfo.enable_photon = $EnablePhoton
    $EndpointInfoJSON = $EndpointInfo | ConvertTo-Json -Depth 32

    if($EndpointId) {
        Return Invoke-PostRESTRequest -URI "${BaseURI}/sql/endpoints/${EndpointId}/edit" -Token $Token -Body $EndpointInfoJSON
    }
}

Function Deploy-SQLEndPoint {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$EndpointName,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$ClusterSize, #2x-Small, X-Small, Small, Medium, Large, X-Large, 2X-Large, 3X-Large, 4X-Large
        [Parameter(Mandatory=$false, Position = 4)]
        [int]$MinNumClusters=1,
        [Parameter(Mandatory=$true, Position = 5)]
        [int]$MaxNumClusters,
        [Parameter(Mandatory=$true, Position = 6)]
        [int]$AutoStopMinutes,
        [Parameter(Mandatory=$false, Position = 7)]
        $tags = @{},
        [Parameter(Mandatory=$false, Position = 8)]
        [bool]$EnablePhoton = $true
    )
    
    $EndpointId = (Get-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName $EndPointName).id
    if(!$EndpointId) {
        New-SQLEndPoint -BaseURI $URIBase -Token $Token -EndpointName $EndpointName -ClusterSize $ClusterSize -MinNumClusters $MinNumClusters -MaxNumClusters $MaxNumClusters -AutoStopMinutes $AutoStopMinutes -tags $tags -EnablePhoton $EnablePhoton
    } else {
        Edit-SQLEndPoint -BaseURI $URIBase -Token $Token -EndpointName $EndpointName -ClusterSize $ClusterSize -MinNumClusters $MinNumClusters -MaxNumClusters $MaxNumClusters -AutoStopMinutes $AutoStopMinutes -tags $tags -EnablePhoton $EnablePhoton
    }
}

