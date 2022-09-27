
((Get-DatabricksRepos -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).repos

((Get-SQLEndpoints -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).endpoints
((Get-SQLEndpointPermissionLevels -BaseURI $URIBase -Token $Token -SQLEndpointId "eb53bc52b7bf52ed").Content | ConvertFrom-Json).permission_levels
$Users = @("eedgeworth@valorem.com")
$Groups = @("admins")
$ServicePrincipals = @()
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId "eb53bc52b7bf52ed" -AccessControlList $AccessControlList
$Response = Reset-SQLEndpointPermissions -BaseURI $URIBase -Token $Token -SQLEndpointId "eb53bc52b7bf52ed" -AccessControlList $AccessControlList

((Get-Models -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).registered_models
((Get-Model -BaseURI $URIBase -Token $Token -ModelName "Train Linear Regression for Table bikesharingml").Content | ConvertFrom-Json).registered_model.latest_versions.run_id
((Get-ModelPermissionLevels -BaseURI $URIBase -Token $Token -ModelId "c9769d36b5734c09a7ea7e23632365e6").Content | ConvertFrom-Json).permission_levels
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-ModelPermissions -BaseURI $URIBase -Token $Token -ModelId "c9769d36b5734c09a7ea7e23632365e6" -AccessControlList $AccessControlList
$Response = Reset-ModelPermissions -BaseURI $URIBase -Token $Token -ModelId "c9769d36b5734c09a7ea7e23632365e6" -AccessControlList $AccessControlList

((Get-Experiments -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).experiments
((Get-ExperimentPermissionLevels -BaseURI $URIBase -Token $Token -ExperimentId "4108393134625351").Content | ConvertFrom-Json).permission_levels
((Get-ExperimentPermissions -BaseURI $URIBase -Token $Token -ExperimentId "4108393134625351").Content | ConvertFrom-Json)
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-ExperimentPermissions -BaseURI $URIBase -Token $Token -ExperimentId "4108393134625351" -AccessControlList $AccessControlList
$Response = Reset-ExperimentPermissions -BaseURI $URIBase -Token $Token -ExperimentId "4108393134625351" -AccessControlList $AccessControlList

((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework").Content | ConvertFrom-Json).objects
((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Repos/Development/DataValueCatalyst/AzureDatabricks/FrameworkNotebooks").Content | ConvertFrom-Json).objects
((Get-DirectoryPermissionLevels -BaseURI $URIBase -Token $Token -DirectoryId "1312083517437588").Content | ConvertFrom-Json).permission_levels
((Get-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId "1312083517437588").Content | ConvertFrom-Json).access_control_list
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId "2247184013453470" -AccessControlList $AccessControlList
$Response = Reset-DirectoryPermissions -BaseURI $URIBase -Token $Token -DirectoryId "2247184013453470" -AccessControlList $AccessControlList

((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Framework/Development").Content | ConvertFrom-Json).objects
((Get-DatabricksWorkbookDirectoryContents -BaseURI $URIBase -Token $Token -NotebookDirectory "/Repos/Development/DataValueCatalyst/AzureDatabricks/FrameworkNotebooks/Development").Content | ConvertFrom-Json).objects
((Get-NotebookPermissionLevels -BaseURI $URIBase -Token $Token -NotebookId "1312083517439805").Content | ConvertFrom-Json).permission_levels
((Get-NotebookPermissions -BaseURI $URIBase -Token $Token -NotebookId "2247184013453126").Content | ConvertFrom-Json).access_control_list
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-NotebookPermissions -BaseURI $URIBase -Token $Token -NotebookId "2247184013453126" -AccessControlList $AccessControlList
$Response = Reset-NotebookPermissions -BaseURI $URIBase -Token $Token -NotebookId "2247184013453126" -AccessControlList $AccessControlList

((Get-DatabricksJobs -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).jobs
((Get-JobPermissionLevels -BaseURI $URIBase -Token $Token -JobId "166329").Content | ConvertFrom-Json).permission_levels
((Get-JobPermissions -BaseURI $URIBase -Token $Token -JobId "166329").Content | ConvertFrom-Json).access_control_list
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-JobPermissions -BaseURI $URIBase -Token $Token -JobId "166329" -AccessControlList $AccessControlList
$Response = Reset-JobPermissions -BaseURI $URIBase -Token $Token -JobId "166329" -AccessControlList $AccessControlList

((Get-InstancePools -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).instance_pools
((Get-PoolPermissionLevels -BaseURI $URIBase -Token $Token -InstancePoolId "1011-162800-wade315-pool-thxd2boz").Content | ConvertFrom-Json).permission_levels
((Get-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId "1011-162800-wade315-pool-thxd2boz").Content | ConvertFrom-Json).access_control_list
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId "1011-162800-wade315-pool-thxd2boz" -AccessControlList $AccessControlList
$Response = Reset-PoolPermissions -BaseURI $URIBase -Token $Token -InstancePoolId "1011-162800-wade315-pool-thxd2boz" -AccessControlList $AccessControlList

((Get-DatabricksClusters -BaseURI $URIBase -Token $Token).Content | ConvertFrom-Json).clusters.cluster_id
((Get-ClusterPermissionLevels -BaseURI $URIBase -Token $Token -ClusterId "1011-163252-knot614").Content | ConvertFrom-Json).permission_levels
(Get-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId "1011-163252-knot614").Content
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("admins","Contributors")
$ServicePrincipals = @("b674c80e-baf9-4aac-ac2c-5e3fb13aa35a")
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_MANAGE" | ConvertTo-Json
$Response = Update-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId "1011-163252-knot614" -AccessControlList $AccessControlList
$Response = Reset-ClusterPermissions -BaseURI $URIBase -Token $Token -ClusterId "1011-163252-knot614" -AccessControlList $AccessControlList

Get-AuthorizationTokenPermissionLevels -BaseURI $URIBase -Token $Token
$Users = @("eedgeworth@valorem.com","dvc-d-mdp-adb-rgsilvergoldzoneowners@valorem.com")
$Groups = @("Contributors")
$ServicePrincipals = @()
$AccessControlList = New-AccessControlList -Users $Users -Groups $Groups -ServicePrincipals $ServicePrincipals -PermissionLevel "CAN_USE" | ConvertTo-Json
$Response = Update-AuthorizationTokenPermissions -BaseURI $URIBase -Token $Token -AccessControlList $AccessControlList