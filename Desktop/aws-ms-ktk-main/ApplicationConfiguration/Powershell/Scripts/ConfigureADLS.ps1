param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $false, Position = 3)]
    [string]$SchemasDirectory = "",
    [Parameter(Mandatory = $false, Position = 4)]
    [string]$DQRulesDirectory = ""
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\ADLS\ADLSGen2Module.ps1

$StorageAccounts = Get-StorageAccounts -ResourceGroupName $ResourceGroupName

#Upload Schemas
if ($SchemasDirectory -ne "") {
    $StorageAccountName = ($StorageAccounts | Where-Object {$_.Kind -eq "StorageV2" -and $_.StorageAccountName -like "*adlsb*"}).StorageAccountName
    Write-Host "Uploading AppInsights schemas to $StorageAccountName/raw/schemas/appinsights" -ForegroundColor Cyan
    $LocalDirectory = "$SchemasDirectory\appinsights"
    Copy-LocalFilesToDirectory -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName -FileSystemName "raw" -LocalDirectory $LocalDirectory -Recurse $true -DestinationDirectory "schemas/appinsights"
}

#Upload Data Quality Rules
if ($DQRulesDirectory -ne "") {
    $StorageAccountName = ($StorageAccounts | Where-Object {$_.StorageAccountName -like "*blobt*"}).StorageAccountName
    Write-Host "Upload Data Quality Rules to $StorageAccountName/staging/goldprotected.vActiveDataQualityRule/goldprotected.vActiveDataQualityRule.json" -ForegroundColor Cyan
    $LocalPath = "$DQRulesDirectory/goldprotected.vActiveDataQualityRule.json"
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    Set-AzStorageBlobContent -Context $Context -File $LocalPath -Blob "goldprotected.vActiveDataQualityRule/goldprotected.vActiveDataQualityRule.json" -Container "staging" -Force
}

throw("exit")