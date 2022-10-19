Set-AzContext -SubscriptionId $SubscriptionId
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksJobs.ps1

$TerminalStates = @("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

Write-Host "$(Get-Date -Format G) : Running UAT_ExternalFiles Job"
$JobRun = Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "UAT_ExternalFiles"
Do {
    Start-Sleep 60
    #Write-Host "$(Get-Date -Format G) : Running Batch Ingestion Job"
    $JobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($JobRun.Content | ConvertFrom-Json).run_id
} While (($JobOutput.Content | ConvertFrom-Json).metadata.state.life_cycle_state -notin $TerminalStates)

$JobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($JobRun.Content | ConvertFrom-Json).run_id

Describe 'UAT_ExternalFiles Testing' {
    Context 'UAT_ExternalFiles' {
        It 'Should Succeed' {
            ($JobOutput.Content | ConvertFrom-Json).metadata.state.result_state | Should -Be "SUCCESS"
        }
    }
}