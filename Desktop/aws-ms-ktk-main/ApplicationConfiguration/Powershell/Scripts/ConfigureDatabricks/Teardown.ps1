param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $false, Position = 26)]
    [string]$DeployDatabricksClusters = "false",
    [Parameter(Mandatory = $false, Position = 28)]
    [string]$DeployJobs = "true"
)

#region begin Teardown
if ($DeployJobs -eq "true") {
    $Jobs = (((Get-DatabricksJobs -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).jobs | Where-Object {$_.creator_user_name -match("^(\{){0,1}[0-9a-fA-F]{8}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{4}\-[0-9a-fA-F]{12}(\}){0,1}$") -eq $true}).settings.name
    ForEach ($JobName in $Jobs) {
        Remove-DatabricksJob -BaseURI $URIBase -Token $Token -JobName $JobName
    }
}

if ($DeployDatabricksClusters -eq "true") {
    #Remove existing Clusters, Jobs, Instance Pools so they can be recreated with latest configuration
    $Clusters = (((Get-DatabricksClusters -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).clusters | Where-Object {$_.cluster_source -eq "API"}).cluster_name
    ForEach ($ClusterName in $Clusters) {
        #Do not remove the high concurrency cluster or instance pool for continuity of Power BI connectivity
        #if ($ClusterName -ne "HighConcurrency") {
            Remove-DatabricksCluster -BaseURI $URIBase -Token $Token -ClusterName $ClusterName
        #}
    }

    Remove-InstancePool -BaseURI $URIBase -Token $Token -Name "Default"
    Remove-InstancePool -BaseURI $URIBase -Token $Token -Name "DeltaEngine"
    Remove-InstancePool -BaseURI $URIBase -Token $Token -Name "Photon"
    Remove-InstancePool -BaseURI $URIBase -Token $Token -Name "HighConcurrency" #Do not remove the high concurrency cluster or instance pool for continuity of Power BI connectivity
}
#endregion
