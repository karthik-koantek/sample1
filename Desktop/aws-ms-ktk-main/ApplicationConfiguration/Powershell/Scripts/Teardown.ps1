[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"

#Remove
#$ServicePrincipal = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
#$ApplicationId = $ServicePrincipal.ApplicationId.Guid
#$KeyId = (Get-AzADAppCredential -DisplayName $ServicePrincipalDisplayName).KeyId
#if($KeyId) {Remove-AzADAppCredential -ApplicationId $ApplicationId -KeyId $KeyId -Force}
#$ServicePrincipal = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
#$KeyId = (Get-ADServicePrincipalCredential -ServicePrincipalDisplayName $ServicePrincipalDisplayName).KeyId
#if($KeyId) {Remove-AzADSpCredential -ServicePrincipalObject $ServicePrincipal -KeyId $KeyId -Force}

#Remove-ADServicePrincipal -ServicePrincipalDisplayName $ServicePrincipalDisplayName

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1

. C:\src\ktkaz\Azure Databricks\ApplicationConfiguration\Powershell\Modules\AzureResources\AzureResourcesModule.ps1
[string]$UniqueString = New-UniqueString -String "sqlsat966" -Salt "Eddie" -Length 3
[string]$BaseString = "sqlsat966"
[string]$StringRoot = "$BaseString$UniqueString"
$StringRoot

Get-AzResourceGroup | Select ResourceGroupName