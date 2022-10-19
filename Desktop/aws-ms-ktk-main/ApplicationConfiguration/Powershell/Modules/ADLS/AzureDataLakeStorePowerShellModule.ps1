$Environment = "dev"
$SubscriptionName = "Analytics $Environment"
$ADLSAccountName = ""

$subscription = Get-AzureRmSubscription -SubscriptionName $SubscriptionName
Select-AzureRmSubscription -Subscription $subscription

function new-ADLSItemACLEntryForADGroup ($groupSuffix)
{
    $ADGroupName = "avnet-$Environment-$groupSuffix"
    $ADGroup = Get-AzureRmADGroup -DisplayName $ADGroupName
    $ADGroup.Id

    Set-AzureRmDataLakeStoreItemAclEntry `
        -AccountName $ADLSAccountName `
        -Path '/' `
        -AceType Group `
        -Recurse `
        -Id $ADGroup.Id `
        -Permissions All

    Set-AzureRmDataLakeStoreItemAclEntry `
        -AccountName $ADLSAccountName2 `
        -Path '/' `
        -AceType Group `
        -Recurse `
        -Id $ADGroup.Id `
        -Permissions All
}

function new-ADLSItemACLEntryForServicePrincipal ($suffix)
{
    $Id = (Get-AzureRmADServicePrincipal -DisplayName "avnet-$Environment-$suffix").Id
    Set-AzureRmDataLakeStoreItemAclEntry `
        -AccountName $ADLSAccountName `
        -Path '/' `
        -AceType User `
        -Recurse `
        -Id $Id `
        -Permissions All

    Set-AzureRmDataLakeStoreItemAclEntry `
        -AccountName $ADLSAccountName2 `
        -Path '/' `
        -AceType User `
        -Recurse `
        -Id $Id `
        -Permissions All
}

new-ADLSItemACLEntryForADGroup "analyticsdev-af-admin-agg"
new-ADLSItemACLEntryForADGroup "analyticsdev-af-contrib-agg"
new-ADLSItemACLEntryForADGroup "analyticsdev-af-read-agg"

new-ADLSItemACLEntryForServicePrincipal "appreg-databricks-admin"
new-ADLSItemACLEntryForServicePrincipal "appreg-databricks-contribute"
new-ADLSItemACLEntryForServicePrincipal "appreg-databricks-read"




