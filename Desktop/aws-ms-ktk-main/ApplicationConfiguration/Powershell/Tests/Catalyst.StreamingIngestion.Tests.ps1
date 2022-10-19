Set-AzContext -SubscriptionId $SubscriptionId
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksJobs.ps1

$TerminalStates = @("TERMINATED", "SKIPPED", "INTERNAL_ERROR")

Write-Host "$(Get-Date -Format G) : Running Streaming Ingestion Job"
$StreamingIngestionJobRun = Invoke-DatabricksJob -BaseURI $URIBase -Token $Token -JobName "Streaming Ingestion Job"
Do {
    Start-Sleep 60
    #Write-Host "$(Get-Date -Format G) :Running Streaming Ingestion Job"
    $StreamingIngestionJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($StreamingIngestionJobRun.Content | ConvertFrom-Json).run_id
} While (($StreamingIngestionJobOutput.Content | ConvertFrom-Json).metadata.state.life_cycle_state -notin $TerminalStates)

$StreamingIngestionJobOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunID ($StreamingIngestionJobRun.Content | ConvertFrom-Json).run_id

Describe 'Streaming Ingestion Testing' {
    Context 'Streaming Ingestion' {
        It 'Should Succeed' {
            ($StreamingIngestionJobOutput.Content | ConvertFrom-Json).metadata.state.result_state | Should -Be "SUCCESS"
        }
    }
}