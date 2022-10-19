Set-AzContext -SubscriptionId $SubscriptionId
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksJobs.ps1

$TerminalStates = @("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

Write-Host "$(Get-Date -Format G) : Running Table Maintenance Job"
$TableMaintenanceJobRun = Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Table Maintenance"
Do {
    Start-Sleep 60
    #Write-Host "$(Get-Date -Format G) : Running Table Maintenance Job"
    $TableMaintenanceJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($TableMaintenanceJobRun.Content | ConvertFrom-Json).run_id
} While (($TableMaintenanceJobOutput.Content | ConvertFrom-Json).metadata.state.life_cycle_state -notin $TerminalStates)

$TableMaintenanceJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($TableMaintenanceJobRun.Content | ConvertFrom-Json).run_id

Describe 'Table Maintenance Testing' {
    Context 'Table Maintenance' {
        It 'Should Succeed' {
            ($TableMaintenanceJobOutput.Content | ConvertFrom-Json).metadata.state.result_state | Should -Be "SUCCESS"
        }
    }
}