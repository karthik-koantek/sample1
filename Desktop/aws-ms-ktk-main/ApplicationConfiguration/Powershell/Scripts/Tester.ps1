[string]$ModulesDirectory = "C:\src\Analytics Accelerator Local\Analytics Accelerator Local\ApplicationConfiguration\Powershell\Modules"
[string]$Environment = "dev"
[int]$Length = 3
[string]$BaseString = "aa"
[string]$Salt = "Eddie"

& C:\src\DatabricksFramework\ApplicationConfiguration\Powershell\Scripts\GenerateUniqueResourceNames.ps1 `
    -BaseString $BaseString `
    -Salt $Salt `
    -Length $Length `
    -ModulesDirectory $ModulesDirectory `
    -Environment $Environment


. C:\src\DatabricksFramework\ApplicationConfiguration\Powershell\Modules\Naming\NamingModule.ps1
[string]$UniqueString = New-UniqueString -String "aadev" -Salt "Eddie" -Length 3
[string]$BaseString = "aadev"
[string]$StringRoot = "$BaseString-$UniqueString"
$StringRoot

[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-d-mdp-ef2-rg"
[string]$AdminUser = "eedgeworth@valorem.com"
[string]$Secret = "rwV24m./uvRy-3@T6wk/2s8jx2cP_vgg"
[string]$DatabricksWorkspaceName = "dvc-d-mdp-ef2-dbws"
[string]$Location = "westus2"
[string]$StorageAccountName = "dvcdmdpef2adls"
[string]$AzureSQLServerName = "dvc-d-mdp-ef2-sql"
[string]$AzureSQLDatabaseName = "Accelerator"
[string]$AzureSQLServerAdministratorLogin = "armdeploymentadmin"
[string]$AzureSQLServerAdministratorPwd = "@armdep10yment@dmin"
[string]$NsgName = "databricks-nsg"
[string]$VnetName = "databricks-vnet"
[string]$PrivateSubnetName = "private-subnet"
[string]$PublicSubnetName = "public-subnet"
[string]$PricingTier = "premium"
[string]$VnetCidr = "10.179.0.0/16"
[string]$PrivateSubnetCidr = "10.179.0.0/18"
[string]$PublicSubnetCidr = "10.179.64.0/18"
[string]$StorageAccountType = "Standard_LRS"
[string]$StorageAccountKind = "StorageV2"
[string]$StorageAccountAccessTier = "Hot"
[bool]$StorageAccountSupportsHttpsTrafficOnly = $true
[string]$NetworkAclsBypass = "AzureServices"
[string]$NetworkAclsDefaultAction = "Deny"
[bool]$IsHnsEnabled = $true
[string]$IpAddressFirewallException = "70.185.39.36"
[string]$CoreARMTemplateFile = "C:\src\DVCatalyst Local\Infrastructure\ARM\DeploymentTemplates\DVCatalyst\Core\armdeployment.json"
[string]$KeyVaultARMTemplateFile = "C:\src\DVCatalyst Local\Infrastructure\ARM\DeploymentTemplates\DVCatalyst\KeyVault\armdeployment.json"
[string]$KeyVaultName = "dvc-d-mdp-ef2-kv"
[string]$KeyVaultSku = "Standard"
[string]$TenantId = "5c8085d9-1e88-4bb6-b5bd-e6e6d5b5babd"
[bool]$EnabledForDeployment = $true
[bool]$EnabledForTemplateDeployment = $true
[bool]$EnabledForDiskEncryption = $true
[bool]$EnableRbacAuthorization = $false
[bool]$EnableSoftDelete = $false
[int32]$SoftDeleteRetentionInDays = 30
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts\RunARMTemplate.ps1' `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -DatabricksWorkspaceName $DatabricksWorkspaceName `
    -Location $Location `
    -StorageAccountName $StorageAccountName `
    -AzureSQLServerName $AzureSQLServerName `
    -AzureSQLDatabaseName $AzureSQLDatabaseName `
    -AzureSQLServerAdministratorLogin $AzureSQLServerAdministratorLogin `
    -AzureSQLServerAdministratorPwd $AzureSQLServerAdministratorPwd `
    -NsgName $NsgName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -PricingTier $PricingTier `
    -VnetCidr $VnetCidr `
    -PrivateSubnetCidr $PrivateSubnetCidr `
    -PublicSubnetCidr $PublicSubnetCidr `
    -StorageAccountType $StorageAccountType `
    -StorageAccountKind $StorageAccountKind `
    -StorageAccountAccessTier $StorageAccountAccessTier `
    -StorageAccountSupportsHttpsTrafficOnly $StorageAccountSupportsHttpsTrafficOnly `
    -NetworkAclsBypass $NetworkAclsBypass `
    -NetworkAclsDefaultAction $NetworkAclsDefaultAction `
    -IsHnsEnabled $IsHnsEnabled `
    -IpAddressFirewallException $IpAddressFirewallException `
    -CoreARMTemplateFile $CoreARMTemplateFile
    -KeyVaultARMTemplateFile = $KeyVaultARMTemplateFile
    -KeyVaultName = $KeyVaultName
    -KeyVaultSku = $KeyVaultSku
    -TenantId = $TenantId
    -EnabledForDeployment $EnabledForDeployment
    -EnabledForTemplateDeployment $EnabledForTemplateDeployment
    -EnabledForDiskEncryption $EnabledForDiskEncryption
    -EnableRbacAuthorization $EnableRbacAuthorization
    -EnableSoftDelete $EnableSoftDelete
    -SoftDeleteRetentionInDays $SoftDeleteRetentionInDays


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


[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-d-mdp-ef2-rg"
[string]$Region = "westus2"
[string]$BearerToken = "dapi2f1dade69920cdb555ef6289e1ba07b8"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
[string]$InitScriptPath = "C:\src\DVCatalyst Local\ApplicationConfiguration\ClusterInit\pyodbc.sh"
[string]$NotebookDirectory = "C:\src\DVCatalyst Local\Notebooks"
[string]$ScriptsDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts"
[string]$FrameworkDBAdminPwd = "@armdep10yment@dmin"
[string]$ExternalSystem = "adventureworkslt"
[string]$SQLServerName = "dbvdc.database.windows.net"
[string]$DatabaseName = "dbvdc"
[string]$Login = "dbvdc"
[string]$Pwd = "@armdep10yment@dmin"
[string]$CosmosDBJARPath = "C:\src\DVCatalyst Local\ApplicationConfiguration\jars\azure-cosmosdb-spark_2.4.0_2.11-2.1.2-uber.jar"
[string]$NodeType = "Standard_DS3_v2"
[string]$SparkVersion = "6.6.x-scala2.11"
& C:\src\DatabricksFramework\ApplicationConfiguration\Powershell\Scripts\ConfigureDatabricks.ps1 `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Region $Region `
    -BearerToken $BearerToken `
    -ModulesDirectory $ModulesDirectory `
    -InitScriptPath $InitScriptPath `
    -FrameworkDBAdminPwd $FrameworkDBAdminPwd `
    -NotebookDirectory $NotebookDirectory

[string]$DWTemplateDatabaseName = "DWTemplate"
[string]$Edition = "Basic" #Basic, Standard, Premium, DataWarehouse, GeneralPurpose, BusinessCritical
& C:\src\DatabricksFramework\ApplicationConfiguration\Powershell\Scripts\ConfigureSQLServer.ps1 `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -ModulesDirectory $ModulesDirectory `
    -DatabaseName $DWTemplateDatabaseName `
    -Edition $Edition


[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-d-mdp-ef2-rg"
[string]$Region = "westus2"
[string]$BearerToken = "dapi3ddf6092d5e07b526465f48bbe4e185c"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
[string]$ScriptsDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts"
[string]$TestsDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Tests"
[string]$StorageAccountName = "dvcdmdpef2adls"
[string]$SQLServerName = "dvc-d-mdp-ef2-sql"
[string]$SQLServerLogin = "armdeploymentadmin"
[string]$SQLServerPwd = "@armdep10yment@dmin"
[string]$MetadataDBName = "Accelerator"
[string]$DWDatabaseName = "DWTemplate"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Tests\RunPesterTests.ps1' `
-SubscriptionId $SubscriptionId `
-ResourceGroupName $ResourceGroupName `
-Region $Region `
-BearerToken $BearerToken `
-ModulesDirectory $ModulesDirectory `
-ScriptsDirectory $ScriptsDirectory `
-TestsDirectory $TestsDirectory `
-StorageAccountName $StorageAccountName `
-SQLServerName $SQLServerName `
-SQLServerLogin $SQLServerLogin `
-SQLServerPwd $SQLServerPwd `
-MetadataDBName $MetadataDBName `
-DWDatabaseName $DWDatabaseName


[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$Region = "westus2"
[string]$BearerToken = "dapib45f8736da594f1ec2bee8ddf433ac9b"
[string]$PowershellDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Tests\RunUATJobs.ps1' `
-SubscriptionId $SubscriptionId `
-Region $Region `
-BearerToken $BearerToken `
-PowershellDirectory $PowershellDirectory


[string]$SubscriptionId = "cb6f8c17-5a8b-41cb-9e84-e3bd57a193de"
[string]$ScriptsDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts\ExternalSystemSecrets\SaveSecretsToExternalKeyVault-dev.ps1' `
    -SubscriptionId $SubscriptionId `
    -ScriptsDirectory $ScriptsDirectory `
    -ModulesDirectory $ModulesDirectory

[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$BearerToken = "dapib45f8736da594f1ec2bee8ddf433ac9b"
[string]$Region = "westus2"
[string]$Machine = "https://$Region.azuredatabricks.net"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
. $ModulesDirectory\Databricks\DatabricksTokens.ps1

((Get-DatabricksTokens -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).token_infos
$ADFBearerToken = Get-DatabricksToken -BaseURI $URIBase -Token $Token -Comment "adf"
$ADFBearerToken = ((New-DatabricksToken -BaseURI $URIBase -Token $Token -Comment "adf").Content)
Revoke-DatabricksToken -BaseURI $URIBase -Token $Token -TokenIdToDelete $ADFBearerToken.token_id

[string]$SubscriptionId = "fdda8511-e870-44c5-960d-554c7a64d427"
[string]$ResourceGroupName = "dvc-d-mdp-ef2-rg"
[string]$Location = "westus2"
[string]$ModulesDirectory = "C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Modules"
[string]$BlobStorageAccountName = "dvcdmdpef2blob"
[string]$VnetName = "databricks-vnet"
[string]$PrivateSubnetName = "private-subnet"
[string]$PublicSubnetName = "public-subnet"
[string]$BlobStorageAccountARMTemplateFile = "C:\src\DVCatalyst Local\Infrastructure\ARM\DeploymentTemplates\DVCatalyst\Blob\armdeployment.json"
& 'C:\src\DVCatalyst Local\ApplicationConfiguration\Powershell\Scripts\RunBlobStorageAccountARMTemplate.ps1' `
    -SubscriptionId $SubscriptionId `
    -ResourceGroupName $ResourceGroupName `
    -Location $Location `
    -ModulesDirectory $ModulesDirectory `
    -BlobStorageAccountName $BlobStorageAccountName `
    -VnetName $VnetName `
    -PrivateSubnetName $PrivateSubnetName `
    -PublicSubnetName $PublicSubnetName `
    -BlobStorageAccountARMTemplateFile $BlobStorageAccountARMTemplateFile