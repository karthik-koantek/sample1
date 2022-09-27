. $PSScriptRoot\Helper.ps1

#region begin ACL
Function New-AccessControlListElement {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ElementType, #user_name, group_name, service_principal_name
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$ElementName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$PermissionLevel
    )
    $ACLElement = @{}
    $ACLElement.$ElementType = $ElementName 
    $ACLElement.permission_level = $PermissionLevel
    return $ACLElement
}

Function New-AccessControlList {
    param 
    (
        [Parameter(Mandatory=$false, Position = 0)]
        [System.Collections.ArrayList]$Users = @(),
        [Parameter(Mandatory=$false, Position = 1)]
        [System.Collections.ArrayList]$Groups = @(),
        [Parameter(Mandatory=$false, Position = 2)]
        [System.Collections.ArrayList]$ServicePrincipals = @(),
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$PermissionLevel
    )
    [System.Collections.ArrayList]$ACLList = @()
    $ACLInfo = @{}
    foreach($User in $Users) {$ACLList.add((New-AccessControlListElement -ElementType "user_name" -ElementName $User -PermissionLevel $PermissionLevel)) | Out-Null} 
    foreach($Group in $Groups) {$ACLList.add((New-AccessControlListElement -ElementType "group_name" -ElementName $Group -PermissionLevel $PermissionLevel)) | Out-Null}
    foreach($ServicePrincipal in $ServicePrincipals) {$ACLList.add((New-AccessControlListElement -ElementType "service_principal_name" -ElementName $ServicePrincipal -PermissionLevel $PermissionLevel)) | Out-Null}
    $ACLInfo.access_control_list = $ACLList
    Return $ACLInfo 
}
#endregion

#region begin Tokens
Function Get-AuthorizationTokenPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/authorization/tokens/permissionLevels" -Token $Token  
}

Function Get-AuthorizationTokenPermissions {
	param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/authorization/tokens" -Token $Token  
}

Function Update-AuthorizationTokenPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [System.Object]$AccessControlList
	)
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/authorization/tokens" -Token $Token -Body $AccessControlList
}

Function Reset-AuthorizationTokenPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [System.Object]$AccessControlList
	)
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/authorization/tokens" -Token $Token -Body $AccessControlList
}
#endregion

#region begin clusters
Function Get-ClusterPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ClusterId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/clusters/${ClusterId}/permissionLevels" -Token $Token  
}

Function Get-ClusterPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ClusterId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/clusters/${ClusterId}" -Token $Token  
}

Function Update-ClusterPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ClusterId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/clusters/${ClusterId}" -Token $Token -Body $AccessControlList
}

Function Reset-ClusterPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ClusterId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/clusters/${ClusterId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin pools 
Function Get-PoolPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$InstancePoolId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/instance-pools/${InstancePoolId}/permissionLevels" -Token $Token  
}

Function Get-PoolPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$InstancePoolId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/instance-pools/${InstancePoolId}" -Token $Token  
}

Function Update-PoolPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$InstancePoolId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/instance-pools/${InstancePoolId}" -Token $Token -Body $AccessControlList
}

Function Reset-PoolPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$InstancePoolId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/instance-pools/${InstancePoolId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin jobs
Function Get-JobPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/jobs/${JobId}/permissionLevels" -Token $Token  
}

Function Get-JobPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/jobs/${JobId}" -Token $Token  
}

Function Update-JobPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/jobs/${JobId}" -Token $Token -Body $AccessControlList
}

Function Reset-JobPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$JobId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/jobs/${JobId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin notebooks
Function Get-NotebookPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$NotebookId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/notebooks/${NotebookId}/permissionLevels" -Token $Token  
}

Function Get-NotebookPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$NotebookId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/notebooks/${NotebookId}" -Token $Token  
}

Function Update-NotebookPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$NotebookId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/notebooks/${NotebookId}" -Token $Token -Body $AccessControlList
}

Function Reset-NotebookPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$NotebookId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/notebooks/${NotebookId}" -Token $Token -Body $AccessControlList
}

#endregion

#region begin directories
Function Get-DirectoryPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$DirectoryId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/directories/${DirectoryId}/permissionLevels" -Token $Token  
}

Function Get-DirectoryPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$DirectoryId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/directories/${DirectoryId}" -Token $Token  
}

Function Update-DirectoryPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$DirectoryId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/directories/${DirectoryId}" -Token $Token -Body $AccessControlList
}

Function Reset-DirectoryPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$DirectoryId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/directories/${DirectoryId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin MLflow experiments 

Function Get-ExperimentPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ExperimentId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/experiments/${ExperimentId}/permissionLevels" -Token $Token  
}

Function Get-ExperimentPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ExperimentId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/experiments/${ExperimentId}" -Token $Token  
}

Function Update-ExperimentPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ExperimentId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/experiments/${ExperimentId}" -Token $Token -Body $AccessControlList
}

Function Reset-ExperimentPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ExperimentId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/experiments/${ExperimentId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin MLflow Registered Models

Function Get-ModelPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ModelId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/registered-models/${ModelId}/permissionLevels" -Token $Token  
}

Function Get-ModelPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ModelId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/registered-models/${ModelId}" -Token $Token  
}

Function Update-ModelPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ModelId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/registered-models/${ModelId}" -Token $Token -Body $AccessControlList
}

Function Reset-ModelPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$ModelId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/registered-models/${ModelId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin SQL Endpoints 

Function Get-SQLEndpointPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndpointId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/sql/endpoints/${SQLEndpointId}/permissionLevels" -Token $Token  
}

Function Get-SQLEndpointPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndpointId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/sql/endpoints/${SQLEndpointId}" -Token $Token  
}

Function Update-SQLEndpointPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndpointId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/sql/endpoints/${SQLEndpointId}" -Token $Token -Body $AccessControlList
}

Function Reset-SQLEndpointPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SQLEndpointId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/sql/endpoints/${SQLEndpointId}" -Token $Token -Body $AccessControlList
}
#endregion

#region begin Repos

Function Get-RepoPermissionLevels {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$RepoId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/repos/${RepoId}/permissionLevels" -Token $Token  
}

Function Get-RepoPermissions {
    param 
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$RepoId
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/permissions/repos/${RepoId}" -Token $Token  
}

Function Update-RepoPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$RepoId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PatchRESTRequest -URI "${BaseURI}/permissions/repos/${RepoId}" -Token $Token -Body $AccessControlList
}

Function Reset-RepoPermissions {
    param 
    (
        [Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$RepoId,
        [Parameter(Mandatory=$true, Position = 3)]
        [System.Object]$AccessControlList 
    )
    Return Invoke-PutRESTRequest -URI "${BaseURI}/permissions/repos/${RepoId}" -Token $Token -Body $AccessControlList
}
#endregion 
