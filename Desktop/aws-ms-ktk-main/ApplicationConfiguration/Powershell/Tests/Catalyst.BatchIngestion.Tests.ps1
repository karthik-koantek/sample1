Set-AzContext -SubscriptionId $SubscriptionId
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksJobs.ps1

$TerminalStates = @("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

Write-Host "$(Get-Date -Format G) : Running Batch Ingestion Job"
$BatchIngestionJobRun = Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Batch Ingestion Job"
Do {
    Start-Sleep 60
    #Write-Host "$(Get-Date -Format G) : Running Batch Ingestion Job"
    $BatchIngestionJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($BatchIngestionJobRun.Content | ConvertFrom-Json).run_id
} While (($BatchIngestionJobOutput.Content | ConvertFrom-Json).metadata.state.life_cycle_state -notin $TerminalStates)

$BatchIngestionJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($BatchIngestionJobRun.Content | ConvertFrom-Json).run_id

Describe 'Batch Ingestion Testing' {
    Context 'Batch Ingestion' {
        It 'Should Succeed' {
            ($BatchIngestionJobOutput.Content | ConvertFrom-Json).metadata.state.result_state | Should -Be "SUCCESS"
        }
    }
}