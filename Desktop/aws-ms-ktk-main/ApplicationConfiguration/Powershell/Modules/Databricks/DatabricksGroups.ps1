. $PSScriptRoot\Helper.ps1

Function Get-DatabricksGroups {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	Return Invoke-GetRESTRequest -URI "${BaseURI}/groups/list" -Token $Token
}

Function New-DatabricksGroup {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName
	)

	$groupInfo = @{}
	$groupInfo.group_name = [string]$GroupName
	$groupInfojson = New-Object -TypeName PSObject -Property $groupInfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/groups/create" -Token $Token -Body $groupInfojson
}

Function Add-DatabricksGroup {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName
	)

	$groups = Get-DatabricksGroups -BaseURI $BaseURI -Token $Token
	$groupNames = ($groups.Content | ConvertFrom-Json).group_names

	If($GroupName -notin $groupNames) {
		New-DatabricksGroup -BaseURI $BaseURI -Token $Token -GroupName $GroupName
	}
}

Function Get-DatabricksGroupMembers {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName
	)

	$groupInfo = @{}
	$groupInfo.group_name = [string]$GroupName
	$groupInfojson = New-Object -TypeName PSObject -Property $groupInfo | ConvertTo-Json

	Return Invoke-GetRESTRequest -URI "${BaseURI}/groups/list-members" -Token $Token -Body $groupInfojson
}

Function Add-DatabricksGroupMembers {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$MemberNames,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$UserType
	)

	$members = $MemberNames.Split(" ")
	ForEach($memberName in $members) {
		Add-DatabricksGroupMember -BaseURI $BaseURI -Token $Token -GroupName $GroupName -MemberName $memberName -UserType $UserType
	}
}
Function New-DatabricksGroupMember {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Name,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$UserType = "GROUP",
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$ParentName
	)

	$groupInfo = @{}
	If($UserType -eq "USER"){
		$groupInfo.user_name = $Name
	}
	If($UserType -eq "GROUP"){
		$groupInfo.group_name = $Name
	}
	$groupInfo.parent_name = $ParentName
	$groupInfojson = New-Object -TypeName PSObject -Property $groupInfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/groups/add-member" -Token $Token -Body $groupInfojson
}

Function Add-DatabricksGroupMember {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$MemberName,
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$UserType
	)

	$groupsList = Get-DatabricksGroupMembers -BaseURI $BaseURI -Token $Token -GroupName $GroupName
	$groupsListmembers = ($groupsList.Content | ConvertFrom-Json).members

	If($MemberName -notin $groupsListmembers.user_name)
	{
		New-DatabricksGroupMember -BaseURI $BaseURI -Token $Token -Name $MemberName -UserType $UserType -ParentName $GroupName
	}
}

Function Remove-DatabricksGroup {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$GroupName
	)

	$groupInfo = @{}
	$groupInfo.group_name = [string]$GroupName
	$groupInfojson = New-Object -TypeName PSObject -Property $groupInfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/groups/delete" -Token $Token -Body $groupInfojson
}

Function Remove-DatabricksGroupMember {
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$Name,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$UserType = "GROUP",
		[Parameter(Mandatory=$true, Position = 4)]
		[string]$ParentName
	)

	$groupInfo = @{}
	If($UserType -eq "USER") {
		$groupInfo.user_name = $Name
	}
	If($UserType -eq "GROUP") {
		$groupInfo.group_name = $Name
	}
	$groupInfo.parent_name = $ParentName
	$groupInfojson = New-Object -TypeName PSObject -Property $groupInfo | ConvertTo-Json

	Return Invoke-PostRESTRequest -URI "${BaseURI}/groups/remove-member" -Token $Token -Body $groupInfojson
}

Function Get-SCIMUsers
{
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	Return Invoke-GetRESTRequest -URI "${BaseURI}/preview/scim/v2/Users" -Token $Token -ContentType "application/scim+json"
}

Function Get-SCIMGroups
{
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)

	Return Invoke-GetRESTRequest -URI "${BaseURI}/preview/scim/v2/Groups" -Token $Token -ContentType "application/scim+json"
}

Function Remove-SCIMUser
{
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$UserName
	)

	$UsersJson = Get-SCIMUsers -BaseURI $BaseURI -Token $Token
	$Users = ($UsersJson | ConvertFrom-Json).Resources
	$UserId = ($Users | Where-Object {$_.userName -eq $UserName}).id
	If($UserId)
	{
		Return Invoke-DeleteRESTRequest -URI "${BaseURI}/preview/scim/v2/Users/${UserId}" -Token $Token -ContentType "application/scim+json" -ExpectedStatusCode 204
	}
}
Function New-SCIMUser
{
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token,
		[Parameter(Mandatory=$true, Position = 2)]
		[string]$UserName,
		[Parameter(Mandatory=$true, Position = 3)]
		[string]$GroupName,
		[Parameter(Position = 4)]
		[bool]$AllowClusterCreate = $false
	)

	$UsersJson = Get-SCIMUsers -BaseURI $BaseURI -Token $Token
	$Users = ($UsersJson | ConvertFrom-Json).Resources
	$User = $Users | Where-Object {$_.userName -eq $UserName} | Select-Object "userName"

	$GroupsJson = Get-SCIMGroups -BaseURI $BaseURI -Token $Token
	$Groups = ($GroupsJson | ConvertFrom-Json).Resources
	$GroupId = ($Groups | Where-Object {$_.displayName -eq $GroupName}).id

	If(!$User)
	{
		$UserInfo = @{}
		$Schemas = @("urn:ietf:params:scim:schemas:core:2.0:User")

		$UserInfo.schemas = $Schemas
		$UserInfo.userName = $UserName

		If($GroupId -ne "")
		{
			$Group = @{}
			$Group.value = $GroupId
			$Groups = @($Group)
			$UserInfo.groups = $Groups
		}

		If($AllowClusterCreate)
		{
			$Entitlements = @{}
			$Entitlements.value = "allow-cluster-create"
			$EntitlementsArray = @($Entitlements)
			$UserInfo.entitlements = $EntitlementsArray
		}

		$UserJson = $UserInfo | ConvertTo-Json -Depth 32
		Return Invoke-PostRESTRequest -URI "${BaseURI}/preview/scim/v2/Users" -Token $Token -Body $UserJson -ContentType "application/scim+json" -ExpectedStatusCode 201
	}
}

Function Get-SCIMServicePrincipals
{
	param
	(
		[Parameter(Mandatory=$true, Position = 0)]
		[string]$BaseURI,
		[Parameter(Mandatory=$true, Position = 1)]
		[securestring]$Token
	)
	Return Invoke-GetRESTRequest -URI "${BaseURI}/preview/scim/v2/ServicePrincipals" -Token $Token -ContentType "application/scim+json"
}
