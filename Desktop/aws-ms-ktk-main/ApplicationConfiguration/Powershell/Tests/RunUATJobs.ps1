param
(
    [Parameter(Mandatory = $false, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $false, Position = 1)]
    [string]$Region,
    [Parameter(Mandatory = $false, Position = 3)]
    [string]$BearerToken,
    [Parameter(Mandatory = $false, Position = 4)]
    [string]$PowershellDirectory,
    [Parameter(Mandatory = $false, Position = 5)]
    [string]$TenantId
)

#Set-AzContext -TenantId $TenantId -SubscriptionId $SubscriptionId
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $PowershellDirectory\Modules\Databricks\DatabricksJobs.ps1

$TerminalStates = @("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

$UATJobs = (((Get-DatabricksJobs -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).jobs.settings | Where-Object {$_.name -like "UAT_*"}).name

ForEach ($JobName in $UATJobs) {
    Write-Host "$(Get-Date -Format G) : Running $JobName Job"
    $JobRun = Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName $JobName
    Do {
        Start-Sleep 60
        Write-Host "$(Get-Date -Format G) : Running $JobName Job"
        $JobOutput = Get-DatabricksJobRun -BaseURI $URIBase -Token $Token -RunID ($JobRun.Content | ConvertFrom-Json).run_id
    } While (($JobOutput.Content | ConvertFrom-Json).state.life_cycle_state -notin $TerminalStates)
}

