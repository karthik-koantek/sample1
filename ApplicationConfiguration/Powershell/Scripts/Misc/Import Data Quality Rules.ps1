$SubscriptionId = "ba3707a6-d45b-4bdd-a975-41fdd1ebb123"
$ResourceGroupName = "ktk-d-mdp-e16-rg"
$ModulesDirectory = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Modules"
$StorageAccountName = "ktkdmdpe16blobt"
$ContainerName = "staging"
$Path = "goldprotected.vActiveDataQualityRule/goldprotected.vActiveDataQualityRule.json"
$LocalPath = "C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Tests\goldprotected.vActiveDataQualityRule.json"

Set-AzContext -SubscriptionId $SubscriptionId
. $ModulesDirectory\ADLS\ADLSGen2Module.ps1

$Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
Set-AzStorageBlobContent -Context $Context -File $LocalPath -Blob $Path -Container $ContainerName -Force

