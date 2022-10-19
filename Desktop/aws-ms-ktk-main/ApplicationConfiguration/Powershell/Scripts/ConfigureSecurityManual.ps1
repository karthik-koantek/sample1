
#The Azure DevOps CI/CD Agent service principal doesn't have permission to run script ConfigureSecurity.ps1, so it must be done manually

#Eddie Local Dev
[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-d-mdp-ef2-rg"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
[string]$AdminUser = "eedgeworth@valorem.com"
[string]$Secret = "Ch_iaD-65LAt6_~lO2Dd2-Izs8R.Tw9ZMj"
[string]$DevOpsServicePrincipalName = "eedgeworthValorem-Analytics Accelerator Local-fdda8511-e870-44c5-960d-554c7a64d427"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts\ConfigureSecurity.ps1' `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -AdminUser $AdminUser `
    -Secret $Secret `
    -DevOpsServicePrincipalName $DevOpsServicePrincipalName

#Eddie Local Prod
[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-p-mdp-ef2-rg"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
[string]$AdminUser = "eedgeworth@valorem.com"
[string]$Secret = "Ch_iaD-65LAt6_~lO2Dd2-Izs8R.Tw9ZMj"
[string]$DevOpsServicePrincipalName = "eedgeworthValorem-Analytics Accelerator Local-fdda8511-e870-44c5-960d-554c7a64d427"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts\ConfigureSecurity.ps1' `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -AdminUser $AdminUser `
    -Secret $Secret `
    -DevOpsServicePrincipalName $DevOpsServicePrincipalName

#DI Sub Dev
[string]$SubscriptionId = "7bb2614e-43e6-46d0-b531-609738a791ef"
[string]$ResourceGroupName = "dvc-d-mdp-adb-rg"
[string]$ModulesDirectory = "C:\src\DITeamDataBricks\DataBricks Framework\ApplicationConfiguration\Powershell\Modules"
[string]$AdminUser = "eedgeworth@valorem.com"
[string]$Secret = "Ch_iaD-65LAt6_~lO2Dd2-Izs8R.Tw9ZMj"
[string]$DevOpsServicePrincipalName = "vcg-DataBricks Framework-7bb2614e-43e6-46d0-b531-609738a791ef"
& 'C:\src\DITeamDataBricks\DataBricks Framework\ApplicationConfiguration\Powershell\Scripts\ConfigureSecurity.ps1' `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -AdminUser $AdminUser `
    -Secret $Secret `
    -DevOpsServicePrincipalName $DevOpsServicePrincipalName