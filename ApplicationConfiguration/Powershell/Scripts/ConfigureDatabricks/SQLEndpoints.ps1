param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [hashtable]$StorageAccountHashUsingSecrets,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$DeploySQLEndpoints,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$TenantId
)

#region begin SQL Endpoints
if ($DeploySQLEndpoints -eq "true") {
    Write-Host "Global SQL Endpoint Configuration"
    Edit-GlobalSQLEndpointConfiguration -BaseURI $URIBase -Token $Token -StorageAccountHash $StorageAccountHashUsingSecrets -UseServicePrincipalAuth $false -TenantId $TenantId

    Write-Host "Deplying 2x-Small SQL Endpoint"
    Deploy-SQLEndPoint -BaseURI $URIBase -Token $Token -EndpointName "2xSmall" -ClusterSize "2X-Small" -MinNumClusters 1 -MaxNumClusters 1 -AutoStopMinutes 10
    Write-Host "Deplying Small SQL Endpoint"
    Deploy-SQLEndPoint -BaseURI $URIBase -Token $Token -EndpointName "Small" -ClusterSize "Small" -MinNumClusters 1 -MaxNumClusters 1 -AutoStopMinutes 10
    Write-Host "Deplying Medium SQL Endpoint"
    Deploy-SQLEndPoint -BaseURI $URIBase -Token $Token -EndpointName "Medium" -ClusterSize "Medium" -MinNumClusters 1 -MaxNumClusters 1 -AutoStopMinutes 10

    Stop-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "2xSmall"
    Stop-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "Small"
    Stop-SQLEndpoint -BaseURI $URIBase -Token $Token -SQLEndPointName "Medium"
} 
#endregion

#region begin Permissions
$Users = @()
$Groups = @("Readers", "Contributors")
$ServicePrincipals = @()
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_USE" | ConvertTo-Json

$SQLEndpoints = ((Get-SQLEndpoints -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).endpoints.id
foreach ($EndpointId in $SQLEndpoints) {
    Update-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId $EndpointId -AccessControlList $AccessControlList
}

#endregion