param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$PrimaryDomain
)

#region begin UsersAndGroups

#Groups
Write-Host "Adding Databricks Groups" -ForegroundColor Cyan
Add-DatabricksGroup -BaseURI $URIBase -Token $Token -GroupName "admins"
Add-DatabricksGroup -BaseURI $URIBase -Token $Token -GroupName "Contributors"
Add-DatabricksGroup -BaseURI $URIBase -Token $Token -GroupName "Readers"

##SCIM Users
Write-Host "Adding Databricks Users" -ForegroundColor Cyan
$AdminGroupName = $ResourceGroupName + "DataLakeOwners@$PrimaryDomain"
$TransientZoneOwnerGroupName = $ResourceGroupName + "TransientZoneOwners@$PrimaryDomain"
$BronzeZoneOwnerGroupName = $ResourceGroupName + "BronzeZoneOwners@$PrimaryDomain"
$SilverGoldZoneOwnerGroupName = $ResourceGroupName + "SilverGoldZoneOwners@$PrimaryDomain"
$SandboxZoneOwnerGroupName = $ResourceGroupName + "SandboxZoneOwners@$PrimaryDomain"
$SilverGoldEncryptedDataOwnerGroupName = $ResourceGroupName + "SilverGoldEncryptedDataOwners@$PrimaryDomain"
$SilverGoldUnencryptedDataOwnerGroupName = $ResourceGroupName + "SilverGoldUnencryptedDataOwners@$PrimaryDomain"
$ContributorGroupName = $ResourceGroupName + "DataLakeContributors@$PrimaryDomain"
$TransientZoneContributorGroupName = $ResourceGroupName + "TransientZoneContributors@$PrimaryDomain"
$BronzeZoneContributorGroupName = $ResourceGroupName + "BronzeZoneContributors@$PrimaryDomain"
$SilverGoldZoneContributorGroupName = $ResourceGroupName + "SilverGoldZoneContributors@$PrimaryDomain"
$SandboxZoneContributorGroupName = $ResourceGroupName + "SandboxZoneContributors@$PrimaryDomain"
$SilverGoldEncryptedDataContributorGroupName = $ResourceGroupName + "SilverGoldEncryptedDataContributors@$PrimaryDomain"
$SilverGoldUnencryptedDataContributorGroupName = $ResourceGroupName + "SilverGoldUnencryptedDataContributors@$PrimaryDomain"
$ReaderGroupName = $ResourceGroupName + "DataLakeReaders@$PrimaryDomain"
$TransientZoneReaderGroupName = $ResourceGroupName + "TransientZoneReaders@$PrimaryDomain"
$BronzeZoneReaderGroupName = $ResourceGroupName + "BronzeZoneReaders@$PrimaryDomain"
$SilverGoldZoneReaderGroupName = $ResourceGroupName + "SilverGoldZoneReaders@$PrimaryDomain"
$SandboxZoneReaderGroupName = $ResourceGroupName + "SandboxZoneReaders@$PrimaryDomain"
$SilverGoldEncryptedDataReaderGroupName = $ResourceGroupName + "SilverGoldEncryptedDataReaders@$PrimaryDomain"
$SilverGoldUnencryptedDataReaderGroupName = $ResourceGroupName + "SilverGoldUnencryptedDataReaders@$PrimaryDomain"
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $AdminGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $TransientZoneOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $BronzeZoneOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldZoneOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SandboxZoneOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldEncryptedDataOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldUnencryptedDataOwnerGroupName -GroupName "admins" -AllowClusterCreate $true
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $ContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $TransientZoneContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $BronzeZoneContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldZoneContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SandboxZoneContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldEncryptedDataContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldUnencryptedDataContributorGroupName -GroupName "Contributors" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $ReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $TransientZoneReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $BronzeZoneReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldZoneReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SandboxZoneReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldEncryptedDataReaderGroupName -GroupName "Readers" -AllowClusterCreate $false
New-SCIMUser -BaseURI $URIBase -Token $Token -UserName $SilverGoldUnencryptedDataReaderGroupName -GroupName "Readers" -AllowClusterCreate $false

#endregion
