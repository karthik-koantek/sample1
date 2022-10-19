param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$DQRulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$RunDataQualityNotebooks
)

#region begin Run Notebooks
if ($RunDataQualityNotebooks -eq "true") {
    if ($DQRulesDirectory -ne "") {
        $clusterdata = Start-DatabricksClusterAwaitUntilStarted -BaseURI $URIBase -Token $Token -ClusterName "SingleNode"
        #Run Import Data Quality Rules Notebook
        Write-Host "Running Import Data Quality Rules Notebook" -ForegroundColor Cyan
        $ClusterId = $clusterdata.cluster_id[0]
        $NotebookPath = "/Framework/Data Quality Rules Engine/Import Data Quality Rules"
        $RunName = "Import DQ Rules"
        $RunInfo = Start-DatabricksNotebook -BaseURI $URIBase -Token $Token -ExistingClusterID $ClusterId -NotebookPath $NotebookPath -NotebookParams @{} -RunName $RunName
        Do {
            $RunId =  ($RunInfo.Content | ConvertFrom-Json).run_id
            $RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
            Write-Host ($RunOutput.Content | ConvertFrom-Json).metadata.state
            Start-Sleep 60
        } While (($RunOutput.Content | ConvertFrom-Json).metadata.state.state_message -eq "In run")
        Start-Sleep 30
        $RunOutput = Get-DatabricksJobRunOutput -BaseURI $URIBase -Token $Token -RunId $RunId
        if(($RunOutput.Content | ConvertFrom-Json).metadata.state.result_state -ne "SUCCESS") {
            throw("Notebook Run $RunName Failed")
        }
    }
}
#endregion