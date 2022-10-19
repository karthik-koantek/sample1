# Create Service Principal and make Owner of RG
# Create AD Groups and assign AdminUser to each
# ADLS IAM Roles (Owner, Contributor, Reader, Storage Blob Data Owner, Storage Blob Data Contributor, Storage Blob Data Reader)
# SQL Server Active Directory Admin, IAM Roles (Owner, Contributor, Reader)
# Databricks IAM Roles (Owner, Contributor, Reader)

param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$AdminUser,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$Secret,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DevOpsServicePrincipalName
)

Set-AzContext -SubscriptionId $SubscriptionId

. $ModulesDirectory\AAD\AADSecurityGroupModule.ps1
. $ModulesDirectory\AzureResources\AzureResourcesModule.ps1
. $ModulesDirectory\ADLS\ADLSGen2Module.ps1
. $ModulesDirectory\KeyVault\KeyVaultModule.ps1

#region begin CI/CD Service Principal
# Create Service Principal and make Owner of RG
Write-Host "Creating Service Principal" -ForegroundColor Cyan
$ServicePrincipalDisplayName = $ResourceGroupName + "CICD"
New-ADServicePrincipal -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName -ServicePrincipalDisplayName $ServicePrincipalDisplayName -Role "Owner"
$ServicePrincipal = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
$Scope = "/subscriptions/$SubscriptionId/resourcegroups/$ResourceGroupName"
New-RoleAssignment -ObjectId $ServicePrincipal.Id -Scope $Scope -RoleDefinitionName "Owner"
$SecureSecret = ConvertTo-SecureString $Secret -AsPlainText -Force
$EndDate = (Get-Date).AddDays(1)
Remove-AllADApplicationCredentials -ApplicationDisplayName $ServicePrincipalDisplayName
New-ADApplicationCredential -ApplicationDisplayName $ServicePrincipalDisplayName -SecurePassword $SecureSecret -EndDate $EndDate
#endgregion

#region begin AD Groups
#Run this to reset all AD groups for the environment (start over)
#$ADGroups = Get-AzADGroup | Where-Object {$_.DisplayName -like "$ResourceGroupName*"}
#foreach ($Group in $ADGroups) {
#    $GroupName = $Group.DisplayName
#    Write-Host "Removing AD Group $GroupName" -ForegroundColor Cyan
#    Remove-AADGroup -AADGroupDisplayName $Group.DisplayName
#}

# Create AD Groups and assign AdminUser to each
Write-Host "Creating AD Groups" -ForegroundColor Cyan
$DataLakeOwnerGroups = "$ResourceGroupName Data Lake Owners","$ResourceGroupName Data Lake Contributors","$ResourceGroupName Data Lake Readers"
$TransientZoneGroups = "$ResourceGroupName Transient Zone Owners","$ResourceGroupName Transient Zone Contributors","$ResourceGroupName Transient Zone Readers"
$BronzeZoneGroups = "$ResourceGroupName Bronze Zone Owners","$ResourceGroupName Bronze Zone Contributors","$ResourceGroupName Bronze Zone Readers"
$SilverZoneGroups = "$ResourceGroupName SilverGold Zone Owners","$ResourceGroupName SilverGold Protected Data Owners","$ResourceGroupName SilverGold General Data Owners","$ResourceGroupName SilverGold Zone Contributors","$ResourceGroupName SilverGold Protected Data Contributors","$ResourceGroupName SilverGold General Data Contributors","$ResourceGroupName SilverGold Zone Readers","$ResourceGroupName SilverGold Protected Data Readers","$ResourceGroupName SilverGold General Data Readers"
$SandboxZoneGroups = "$ResourceGroupName Sandbox Zone Owners","$ResourceGroupName Sandbox Zone Contributors","$ResourceGroupName Sandbox Zone Readers"
$PlatinumZoneGroups = "$ResourceGroupName Sanctioned Zone Owners","$ResourceGroupName Sanctioned Zone Contributors","$ResourceGroupName Sanctioned Zone Readers"
$ADGroups = $DataLakeOwnerGroups, $TransientZoneGroups, $BronzeZoneGroups, $SilverZoneGroups, $SandboxZoneGroups, $PlatinumZoneGroups

foreach ($GroupZone in $ADGroups) {
    foreach($Group in $GroupZone) {
        $MailNickName = $Group -Replace " ", ""
        Write-Host "Creating AD Group $Group"
        New-AADGroup -AADGroupDisplayName $Group -MailNickName $MailNickName
        Add-AADMember -AADGroupDisplayName $Group -MemberUserPrincipalName $AdminUser
    }
}

Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Owners" -TargetGroupDisplayName "$ResourceGroupName Transient Zone Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Owners" -TargetGroupDisplayName "$ResourceGroupName Bronze Zone Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Owners" -TargetGroupDisplayName "$ResourceGroupName SilverGold Zone Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Owners" -TargetGroupDisplayName "$ResourceGroupName Sandbox Zone Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Contributors" -TargetGroupDisplayName "$ResourceGroupName Transient Zone Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Contributors" -TargetGroupDisplayName "$ResourceGroupName Bronze Zone Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Contributors" -TargetGroupDisplayName "$ResourceGroupName SilverGold Zone Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Contributors" -TargetGroupDisplayName "$ResourceGroupName Sandbox Zone Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Readers" -TargetGroupDisplayName "$ResourceGroupName Transient Zone Readers"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Readers" -TargetGroupDisplayName "$ResourceGroupName Bronze Zone Readers"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Readers" -TargetGroupDisplayName "$ResourceGroupName SilverGold Zone Readers"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName Data Lake Readers" -TargetGroupDisplayName "$ResourceGroupName Sandbox Zone Readers"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Owners" -TargetGroupDisplayName "$ResourceGroupName SilverGold Protected Data Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Owners" -TargetGroupDisplayName "$ResourceGroupName SilverGold General Data Owners"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Contributors" -TargetGroupDisplayName "$ResourceGroupName SilverGold Protected Data Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Contributors" -TargetGroupDisplayName "$ResourceGroupName SilverGold General Data Contributors"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Readers" -TargetGroupDisplayName "$ResourceGroupName SilverGold Protected Data Readers"
Add-ADGroupMember -GroupDisplayName "$ResourceGroupName SilverGold Zone Readers" -TargetGroupDisplayName "$ResourceGroupName SilverGold General Data Readers"
#endregion

#region begin Storage Accounts
# ADLS IAM Roles (Owner, Contributor, Reader, Storage Blob Data Owner, Storage Blob Data Contributor, Storage Blob Data Reader)
Write-Host "IAM Roles for Azure Data Lake Storage Accounts" -ForegroundColor Cyan
$StorageAccounts = Get-StorageAccounts -ResourceGroupName $ResourceGroupName
$TransientStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*blobt"}
$BronzeStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlsb"}
$SilverGoldStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssg"}
$SandboxStorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -like "*adlssb"}

#region begin Storage Containers/File Systems
#Create Blob Storage Containers (Transient Zone)
Write-Host "Creating Blob Storage Containers (Transient Zone)" -ForegroundColor Cyan
$BlobContainers = (Get-AzStorageContainer -Context $TransientStorageAccount.Context).Name
if("polybase" -notin $BlobContainers) {New-AzStorageContainer -Name "polybase" -Context $TransientStorageAccount.Context}
if("staging" -notin $BlobContainers) {New-AzStorageContainer -Name "staging" -Context $TransientStorageAccount.Context}
IF("azuredatafactory" -notin $BlobContainers) {New-AzStorageContainer -Name "azuredatafactory" -Context $TransientStorageAccount.Context}

#Create File System (Bronze Zone)
Write-Host "Creating File System (Bronze Zone)" -ForegroundColor Cyan
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $BronzeStorageAccount.StorageAccountName -FileSystemName "raw"

#Create File Systems (Silver/Gold Zone)
Write-Host "Creating File System (Silver/Gold Zone)" -ForegroundColor Cyan
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccount.StorageAccountName -FileSystemName "silverprotected"
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccount.StorageAccountName -FileSystemName "silvergeneral"
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccount.StorageAccountName -FileSystemName "goldprotected"
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccount.StorageAccountName -FileSystemName "goldgeneral"
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccount.StorageAccountName -FileSystemName "archive"

#Create File System (Sandbox Zone)
New-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $SandboxStorageAccount.StorageAccountName -FileSystemName "defaultsandbox"
#create and secure additional file systems in the sandbox zone as required
#endregion

#region begin IAM Roles

#region begin Transient Zone IAM Roles
#Transient Zone
$TransientStorageAccountName = $TransientStorageAccount.StorageAccountName
$TransientStorageAccountScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$TransientStorageAccountName"
$TransientZoneOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Transient Zone Owners"
$TransientZoneContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Transient Zone Contributors"
$TransientZoneReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Transient Zone Readers"
New-RoleAssignment -ObjectId $TransientZoneOwnerGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId $TransientZoneOwnerGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Storage Blob Data Owner"
New-RoleAssignment -ObjectId $TransientZoneContributorGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId $TransientZoneContributorGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Storage Blob Data Contributor"
New-RoleAssignment -ObjectId $TransientZoneReaderGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId $TransientZoneReaderGroup.Id -Scope $TransientStorageAccountScope -RoleDefinitionName "Storage Blob Data Reader"
#endregion

#region begin Bronze Zone IAM Roles
#Bronze Zone
$BronzeStorageAccountName = $BronzeStorageAccount.StorageAccountName
$BronzeStorageAccountScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$BronzeStorageAccountName"
$BronzeZoneOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Bronze Zone Owners"
$BronzeZoneContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Bronze Zone Contributors"
$BronzeZoneReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Bronze Zone Readers"
New-RoleAssignment -ObjectId $BronzeZoneOwnerGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId $BronzeZoneOwnerGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Storage Blob Data Owner"
New-RoleAssignment -ObjectId $BronzeZoneContributorGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId $BronzeZoneContributorGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Storage Blob Data Contributor"
New-RoleAssignment -ObjectId $BronzeZoneReaderGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId $BronzeZoneReaderGroup.Id -Scope $BronzeStorageAccountScope -RoleDefinitionName "Storage Blob Data Reader"
#endregion

#region begin SilverGold IAM Roles
#SilverGold Zone
$SilverGoldStorageAccountName = $SilverGoldStorageAccount.StorageAccountName
$SilverGoldStorageAccountScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$SilverGoldStorageAccountName"
$SilverGoldZoneOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Zone Owners"
$SilverGoldZoneContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Zone Contributors"
$SilverGoldZoneReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Zone Readers"
$SilverGoldZoneUnencryptedOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold General Data Owners"
$SilverGoldZoneEncryptedOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Protected Data Owners"
$SilverGoldZoneUnencryptedContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold General Data Contributors"
$SilverGoldZoneEncryptedContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Protected Data Contributors"
$SilverGoldZoneUnencryptedReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold General Data Readers"
$SilverGoldZoneEncryptedReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName SilverGold Protected Data Readers"
New-RoleAssignment -ObjectId $SilverGoldZoneOwnerGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId $SilverGoldZoneOwnerGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Storage Blob Data Owner"
New-RoleAssignment -ObjectId $SilverGoldZoneContributorGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId $SilverGoldZoneContributorGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Storage Blob Data Contributor"
New-RoleAssignment -ObjectId $SilverGoldZoneReaderGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId $SilverGoldZoneReaderGroup.Id -Scope $SilverGoldStorageAccountScope -RoleDefinitionName "Storage Blob Data Reader"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "r--" -Id $SilverGoldZoneReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "r--" -Id $SilverGoldZoneReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "r--" -Id $SilverGoldZoneReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "r--" -Id $SilverGoldZoneReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneEncryptedOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneEncryptedContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silverprotected" -Path "/" -Permission "r--" -Id $SilverGoldZoneEncryptedReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneEncryptedOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "rwx" -Id $SilverGoldZoneEncryptedContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldprotected" -Path "/" -Permission "r--" -Id $SilverGoldZoneEncryptedReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneUnencryptedOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneUnencryptedContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "silvergeneral" -Path "/" -Permission "r--" -Id $SilverGoldZoneUnencryptedReaderGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneUnencryptedOwnerGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "rwx" -Id $SilverGoldZoneUnencryptedContributorGroup.Id -AccessControlType "user"
Set-ACLDefaultScopePermissionsForItem -ResourceGroupName $ResourceGroupName -StorageAccountName $SilverGoldStorageAccountName -FileSystemName "goldgeneral" -Path "/" -Permission "r--" -Id $SilverGoldZoneUnencryptedReaderGroup.Id -AccessControlType "user"
#endregion

#region begin Sandbox IAM Roles
#Sandbox Zone
$SandboxStorageAccountName = $SandboxStorageAccount.StorageAccountName
$SandboxStorageAccountScope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Storage/storageAccounts/$SandboxStorageAccountName"
$SandboxZoneOwnerGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Sandbox Zone Owners"
$SandboxZoneContributorGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Sandbox Zone Contributors"
$SandboxZoneReaderGroup = Get-AADGroup -AADGroupDisplayName "$ResourceGroupName Sandbox Zone Readers"
New-RoleAssignment -ObjectId $SandboxZoneOwnerGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId $SandboxZoneOwnerGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Storage Blob Data Owner"
New-RoleAssignment -ObjectId $SandboxZoneContributorGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId $SandboxZoneContributorGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Storage Blob Data Contributor"
New-RoleAssignment -ObjectId $SandboxZoneReaderGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId $SandboxZoneReaderGroup.Id -Scope $SandboxStorageAccountScope -RoleDefinitionName "Storage Blob Data Reader"
#endregion

#endregion

#endregion

#region begin Azure SQL
# SQL Server Active Directory Admin, IAM Roles (Owner, Contributor, Reader)
Write-Host "IAM Roles for Azure SQL Servers" -ForegroundColor Cyan
$SQLServers = Get-AzSqlServer -ResourceGroupName $ResourceGroupName
foreach($SQLServer in $SQLServers) {
    $SQLServerName = $SQLServer.ServerName
    $Scope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Sql/servers/$SQLServerName"

    $SQLADAdmin = Get-AzSqlServerActiveDirectoryAdministrator -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName
    if(!$SQLADAdmin) {
        Set-AzSqlServerActiveDirectoryAdministrator -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName -DisplayName "$ResourceGroupName Sanctioned Zone Owners"
    } else {
        Write-Host "SQL AD Administrator already exists."
    }

    $SanctionedZoneOwnerGroupName = "$ResourceGroupName Sanctioned Zone Owners"
    $SanctionedZoneOwnerGroup = Get-AADGroup -AADGroupDisplayName $SanctionedZoneOwnerGroupName
    New-RoleAssignment -ObjectId $SanctionedZoneOwnerGroup.Id -Scope $Scope -RoleDefinitionName "Owner"

    $SanctionedZoneContributorGroupName = "$ResourceGroupName Sanctioned Zone Contributors"
    $SanctionedZoneContributorGroup = Get-AADGroup -AADGroupDisplayName $SanctionedZoneContributorGroupName
    New-RoleAssignment -ObjectId $SanctionedZoneContributorGroup.Id -Scope $Scope -RoleDefinitionName "Contributor"

    $SanctionedZoneReaderGroupName = "$ResourceGroupName Sanctioned Zone Readers"
    $SanctionedZoneReaderGroup = Get-AADGroup -AADGroupDisplayName $SanctionedZoneReaderGroupName
    New-RoleAssignment -ObjectId $SanctionedZoneReaderGroup.Id -Scope $Scope -RoleDefinitionName "Reader"
}
#endregion

#region begin Databricks
# Databricks IAM Roles (Owner, Contributor, Reader)
Write-Host "IAM Roles for Azure Databricks" -ForegroundColor Cyan
$DatabricksWorkspace = Get-AzResource -ResourceGroupName $ResourceGroupName -ResourceType "Microsoft.Databricks/workspaces"
$DatabricksWorkspaceName = $DatabricksWorkspace.Name
$Scope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.Databricks/workspaces/$DatabricksWorkspaceName"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Data Lake Owners").Id -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Transient Zone Owners").Id -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Bronze Zone Owners").Id -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName SilverGold Zone Owners").Id -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Sandbox Zone Owners").Id -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Data Lake Contributors").Id -Scope $Scope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Transient Zone Contributors").Id -Scope $Scope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Bronze Zone Contributors").Id -Scope $Scope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName SilverGold Zone Contributors").Id -Scope $Scope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Sandbox Zone Contributors").Id -Scope $Scope -RoleDefinitionName "Contributor"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Data Lake Readers").Id -Scope $Scope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Transient Zone Readers").Id -Scope $Scope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Bronze Zone Readers").Id -Scope $Scope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName SilverGold Zone Readers").Id -Scope $Scope -RoleDefinitionName "Reader"
New-RoleAssignment -ObjectId (Get-AzADGroup -DisplayName "$ResourceGroupName Sandbox Zone Readers").Id -Scope $Scope -RoleDefinitionName "Reader"
#endregion

#region begin Key Vault
Write-Host "IAM Roles and Access Policies for Key Vault"
$DataLakeOwnerGroupName = "$ResourceGroupName Data Lake Owners"
$DataLakeOwnerGroup = Get-AADGroup -AADGroupDisplayName $DataLakeOwnerGroupName
$KeyVaultName = (Get-AzKeyVault -ResourceGroupName $ResourceGroupName).VaultName
$Scope = "/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroupName/providers/Microsoft.KeyVault/vaults/$KeyVaultName"
New-RoleAssignment -ObjectId $DataLakeOwnerGroup.Id -Scope $Scope -RoleDefinitionName "Owner"
Set-AzKeyVaultAccessPolicy `
    -VaultName $KeyVaultName `
    -ResourceGroupName $ResourceGroupName `
    -ObjectId $DataLakeOwnerGroup.Id `
    -PermissionsToSecrets get,list,set,delete,backup,restore,recover,purge
#endregion

#region begin Data Factory
$DataFactoryManagedIdentity = (Get-AzDataFactoryV2 -ResourceGroupName $ResourceGroupName).Identity.PrincipalId
Set-AzKeyVaultAccessPolicy `
    -VaultName $KeyVaultName `
    -ResourceGroupName $ResourceGroupName `
    -ObjectId $DataFactoryManagedIdentity `
    -PermissionsToSecrets get,list
#Grant IAM Contributor or owner permissiosn to ADLS Gen2 and Blob Storage, to Databricks, and to Azure SQL Server (or the entire RG)
$Scope = "/subscriptions/$SubscriptionId/resourcegroups/$ResourceGroupName"
New-RoleAssignment -ObjectId $DataFactoryManagedIdentity -Scope $Scope -RoleDefinitionName "Owner"
New-RoleAssignment -ObjectId $DataFactoryManagedIdentity -Scope $Scope -RoleDefinitionName "Storage Blob Data Owner"
#endregion