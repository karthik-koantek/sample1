Function Get-AADGroup {
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName
    )
    Get-AzADGroup -DisplayName $AADGroupDisplayName
}

Function New-AADGroup {
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName,
        [Parameter(Mandatory = $true,Position = 1)]
        [string] $MailNickName
    )
    $SG = Get-AADGroup -AADGroupDisplayName $AADGroupDisplayName
    if (!$SG){
        New-AzADGroup -DisplayName $AADGroupDisplayName -MailNickname $MailNickName
    }
    else{
        Write-Host "AAD Security Group already exists"
    }
}

Function Remove-AADGroup{
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName
    )
    $SG = Get-AADGroup  -AADGroupDisplayName $AADGroupDisplayName
    if ($SG){
    Remove-AzADGroup  -DisplayName $AADGroupDisplayName -PassThru -Force
    }
    else{
        Write-Host "AAD Security Group does not exist"
    }
}

function Get-AllAADGroupMembers{
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName

    )
    $Members = Get-AADGroup -AADGroupDisplayName $AADGroupDisplayName | Get-AzADGroupMember
    return $Members
}

function  Get-AADMember{
    param (
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName,
        [Parameter(Mandatory = $false,Position = 1)]
        [string] $UserDisplayName,
        [Parameter(Mandatory = $false,Position = 2)]
        [string] $UserId,
        [Parameter(Mandatory = $false,Position = 3)]
        [string] $MemberUserPrincipalName

    )
    $Members = (Get-AllAADGroupMembers -AADGroupDisplayName $AADGroupDisplayName)

    if ($UserDisplayName){
        $Member = $Members | Where-Object {$_.DisplayName -eq $UserDisplayName }
        return $Member
    }
    elseif ($UserId){
        $Member = $Members | Where-Object {$_.Id -eq $UserId }
        return $Member
    }
    elseif ($MemberUserPrincipalName){
        $Member = $Members | Where-Object {$_.UserPrincipalName -eq $MemberUserPrincipalName }
        return $Member
    }
    else{
        Write-Host "Provide a valid UserDisplayName or UserId or UserPrincipalName"
    }
}

function Add-ADGroupMember {
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$GroupDisplayName,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$TargetGroupDisplayName
    )
    $Group = Get-AzADGroup -DisplayName $GroupDisplayName
    $TargetGroup = Get-AzADGroup -DisplayName $TargetGroupDisplayName

    $found = Get-AzAdGroupMember -GroupObjectId $TargetGroup.Id | Where-Object {$_.DisplayName -eq $GroupDisplayName}
    if(!$found) {
        Add-AzADGroupMember -MemberObjectId $Group.Id -TargetGroupObjectId $TargetGroup.Id
    } else {
        Write-Host "Group Member $GroupDisplayName is already a member of Group $TargetGroupDisplayName"
    }
}

# Member should exist in the tenant to be added to a security group
function  Add-AADMember {
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName,
        [Parameter(Mandatory = $false,Position = 1)]
        [string] $UserDisplayName,
        [Parameter(Mandatory = $false,Position = 2)]
        [string] $UserId,
        [Parameter(Mandatory = $false,Position = 3)]
        [string] $MemberUserPrincipalName
    )
    if ($UserDisplayName){
        $Member = Get-AADMember -AADGroupDisplayName $AADGroupDisplayName -UserDisplayName $UserDisplayName
        if (!$Member) {
            $UP = (Get-AzADUser -DisplayName $UserDisplayName).UserPrincipalName
            Add-AzADGroupMember -MemberUserPrincipalName $UP  -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member already exists in the AAD Group"
        }
    }
    elseif($UserId){
        $Member = Get-AADMember -AADGroupDisplayName $AADGroupDisplayName -UserId $UserId
        if (!$Member) {
            Add-AzADGroupMember -MemberObjectId $UserId  -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member already exists in the AAD Group"
        }

    }
    elseif($MemberUserPrincipalName){
        $Member = Get-AADMember -AADGroupDisplayName $AADGroupDisplayName -MemberUserPrincipalName $MemberUserPrincipalName
        if (!$Member) {
            Add-AzADGroupMember -MemberUserPrincipalName $MemberUserPrincipalName -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member already exists in the AAD Group"
        }
    }
    else{
        Write-Host "Provide a valid UserDisplayName or UserId or UserPrincipalName"
    }
}

function Remove-Member{
    param(
        [Parameter(Mandatory = $true,Position = 0)]
        [string] $AADGroupDisplayName,
        [Parameter(Mandatory = $false,Position = 1)]
        [string] $UserDisplayName,
        [Parameter(Mandatory = $false,Position = 2)]
        [string] $UserId,
        [Parameter(Mandatory = $false,Position = 3)]
        [string] $MemberUserPrincipalName
    )
    if ($UserDisplayName){
        $Member = Get-Member -AADGroupDisplayName $AADGroupDisplayName -UserDisplayName $UserDisplayName
        if ($Member) {
            $UP = (Get-AzADUser -DisplayName $UserDisplayName).UserPrincipalName
            Remove-AzADGroupMember -MemberUserPrincipalName $UP  -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member does not exist in the AAD Group"
        }
    }
    elseif($UserId){
        $Member = Get-Member -AADGroupDisplayName $AADGroupDisplayName -UserId $UserId
        if ($Member) {
            Remove-AzADGroupMember -MemberObjectId $UserId  -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member does not exist in the AAD Group"
        }

    }
    elseif($MemberUserPrincipalName){
        $Member = Get-Member -AADGroupDisplayName $AADGroupDisplayName -MemberUserPrincipalName $MemberUserPrincipalName
        if ($Member) {
            Remove-AzADGroupMember -MemberUserPrincipalName $MemberUserPrincipalName  -TargetGroupDisplayName $AADGroupDisplayName
        }
        else{
            Write-Host "Member does not exist in the AAD Group"
        }
    }
    else{
        Write-Host "Provide a valid UserDisplayName or UserId or UserPrincipalName"
    }
}

function New-ADServicePrincipal {
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $SubscriptionId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string] $ResourceGroupName,
        [Parameter(Mandatory = $true, Position = 2)]
        [string] $ServicePrincipalDisplayName,
        [Parameter(Mandatory = $true, Position = 3)]
        [string] $Role
    )
    $SP = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
    if(!$SP) {
        $Scope = "/subscriptions/$SubscriptionId/resourcegroups/$ResourceGroupName"
        New-AzADServicePrincipal -Role $Role -Scope $Scope -DisplayName $ServicePrincipalDisplayName
    } else {
        Write-Host "Service Principal already exists."
    }
}

function Remove-ADApplication {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $ServicePrincipalDisplayName
    )
    $ADApplication = Get-AzADApplication -DisplayName $ServicePrincipalDisplayName
    if($ADApplication) {
        $ApplicationId = $ADApplication.ApplicationId.Guid
        Remove-AzADApplication -ApplicationId $ApplicationId -Force
    } else {
        Write-Host "AD Application does not exist."
    }
}

function Remove-ADServicePrincipal {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $ServicePrincipalDisplayName
    )
    $SP = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
    if($SP) {
        $ApplicationId = $SP.ApplicationId.Guid
        Remove-AzADServicePrincipal -ApplicationId $ApplicationId -Force
        Remove-ADApplication -ServicePrincipalDisplayName $ServicePrincipalDisplayName
    } else {
        Write-Host "Service Principal does not exist."
    }
}

function Get-ADServicePrincipalCredential {
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $ServicePrincipalDisplayName
    )
    $SP = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
    if($SP) {
        Get-AzADSpCredential -DisplayName $ServicePrincipalDisplayName
    } else {
        Write-Host "Service Principal does not exist."
    }
}

function New-ADServicePrincipalCredential {
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string] $ServicePrincipalDisplayName
    )
    $SP = Get-AzADServicePrincipal -DisplayName $ServicePrincipalDisplayName
    if($SP) {
        $Credential = $SP | New-AzADSpCredential
    } else {
        Write-Host "Service Principal does not exist."
    }
    return $Credential
}

function New-ADApplicationCredential {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ApplicationDisplayName,
        [Parameter(Mandatory = $true, Position = 1)]
        [securestring]$SecurePassword,
        [Parameter(Mandatory = $true, Position = 2)]
        [datetime]$EndDate
    )
    $Application = Get-AzADApplication -DisplayName $ApplicationDisplayName
    if($Application) {
        New-AzADAppCredential -ObjectId $Application.ObjectId -Password $SecurePassword -EndDate $EndDate
    } else {
        Write-Host "Application does not exist."
    }
}

function Remove-ADApplicationCredential {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ApplicationDisplayName,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$KeyId
    )
    $Application = Get-AzADApplication -DisplayName $ApplicationDisplayName
    $Credential = Get-AzADAppCredential -DisplayName $ApplicationDisplayName | Where-Object {$_.KeyId -eq $KeyId}
    if($Credential) {
        Remove-AzADAppCredential -ObjectId $Application.ObjectId -KeyId $KeyId -Force
    } else {
        Write-Host "Credential does not exist."
    }
}

function Remove-AllADApplicationCredentials {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ApplicationDisplayName
    )
    $Credentials = Get-AzADAppCredential -DisplayName $ApplicationDisplayName
    foreach ($Credential in $Credentials) {
        Remove-ADApplicationCredential -ApplicationDisplayName $ApplicationDisplayName -KeyId $Credential.KeyId
    }
}