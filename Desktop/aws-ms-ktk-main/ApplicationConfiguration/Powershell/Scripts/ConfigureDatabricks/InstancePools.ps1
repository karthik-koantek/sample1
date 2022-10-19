param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$SparkVersion,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$PhotonVersion,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$MLRuntimeSparkVersion,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$NodeType,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$DeltaEngineNodeType,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$DeployDatabricksClusters
)

#region begin InstancePools
if ($DeployDatabricksClusters -eq "true") {
    Write-Host "Creating Default Instance Pool" -ForegroundColor Cyan
    New-InstancePool -BaseURI $URIBase -Token $Token -Name "Default" -MinIdle 0 -MaxCapacity 3 -NodeTypeID $NodeType -IdleMinutes 10 -SparkVersion $SparkVersion
    Write-Host "Creating Delta Engine Instance Pool" -ForegroundColor Cyan
    New-InstancePool -BaseURI $URIBase -Token $Token -Name "Photon" -MinIdle 0 -MaxCapacity 3 -NodeTypeID $DeltaEngineNodeType -IdleMinutes 10 -SparkVersion $PhotonVersion
    Write-Host "Creating High Concurrency Instance Pool" -ForegroundColor Cyan
    New-InstancePool -BaseURI $URIBase -Token $Token -Name "HighConcurrency" -MinIdle 0 -MaxCapacity 3 -NodeTypeID $NodeType -IdleMinutes 10 -SparkVersion $MLRuntimeSparkVersion
}

#endregion

#region begin Permissions
$Users = @()
$Groups = @("Contributors", "Readers")
$ServicePrincipals = @()
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_ATTACH_TO" | ConvertTo-Json

$DefaultInstancePoolId = Get-InstancePool -BaseURI $URIBase -Token $Token -InstancePoolName "Default"
Update-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $DefaultInstancePoolId -AccessControlList $AccessControlList

$PhotonInstancePoolId = Get-InstancePool -BaseURI $URIBase -Token $Token -InstancePoolName "Photon"
Update-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $PhotonInstancePoolId -AccessControlList $AccessControlList

$HighConcurrencyInstancePoolId = Get-InstancePool -BaseURI $URIBase -Token $Token -InstancePoolName "HighConcurrency"
Update-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId $HighConcurrencyInstancePoolId -AccessControlList $AccessControlList

#endregion 
