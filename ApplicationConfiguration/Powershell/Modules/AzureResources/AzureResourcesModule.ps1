
function New-RoleAssignment {
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ObjectId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$RoleDefinitionName,
        [Parameter(Mandatory = $true, Position = 2)]
        [string]$Scope
    )
    $RoleAssignment = Get-AzRoleAssignment -ObjectId $ObjectId -Scope $Scope | Where-Object {$_.RoleDefinitionName -eq $RoleDefinitionName}
    if(!$RoleAssignment) {
        New-AzRoleAssignment -ObjectId $ObjectId -Scope $Scope -RoleDefinitionName $RoleDefinitionName
    } else {
        Write-Host "Object $ObjectId is already a member of the $RoleDefinitionName role for scope $Scope"
    }
}

function Remove-RoleAssignment {
    param (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ObjectId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$RoleDefinitionName,
        [Parameter(Mandatory = $true, Position = 2)]
        [string]$Scope
    )
    $RoleAssignment = Get-AzRoleAssignment -ObjectId $ObjectId -Scope $Scope | Where-Object {$_.RoleDefinitionName -eq $RoleDefinitionName}
    if($RoleAssignment) {
        Remove-AzRoleAssignment -ObjectId $ObjectId -Scope $Scope -RoleDefinitionName $RoleDefinitionName
    } else {
        Write-Host "Object $ObjectId is not a member of the $RoleDefinitionName role for scope $Scope"
    }

}

function New-ResourceGroupDeployment {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$TemplateFile,
        [Parameter(Mandatory = $false, Position = 2)]
        [string]$TemplateParametersFile,
        [Parameter(Mandatory = $false, Position = 3)]
        [System.Object]$TemplateParameterObject,
        [Parameter(Mandatory = $true, Position = 4)]
        [string]$Mode
    )
    if ($TemplateParametersFile) {
        New-AzResourceGroupDeployment `
        -ResourceGroupName $ResourceGroupName `
        -Mode $Mode `
        -TemplateFile $TemplateFile `
        -TemplateParameterFile $TemplateParametersFile
    }

    if ($TemplateParameterObject) {
        New-AzResourceGroupDeployment `
        -ResourceGroupName $ResourceGroupName `
        -Mode $Mode `
        -TemplateFile $TemplateFile `
        -TemplateParameterObject $TemplateParameterObject
    }
}

Function Get-ResourceGroup {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$SubscriptionId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$ResourceGroupName
    )
    $ResourceGroups = Get-AzResourceGroup
    $ResourceGroup = $ResourceGroups | Where-Object {$_.ResourceGroupName -eq $ResourceGroupName}
    return $ResourceGroup
}

Function New-ResourceGroup {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$SubscriptionId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory = $true, Position = 4)]
        [string]$Location
    )
    $ResourceGroup = Get-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName

    If(!$ResourceGroup) {
        Write-Host "Creating Resource Group $ResourceGroupName"
        New-AzResourceGroup -Name $ResourceGroupName -Location $Location
    } else {
        Write-Host "Resource Group exists"
    }
}

Function Remove-ResourceGroup {
    param
    (
        [Parameter(Mandatory = $true, Position = 0)]
        [string]$SubscriptionId,
        [Parameter(Mandatory = $true, Position = 1)]
        [string]$ResourceGroupName
    )
    $ResourceGroup = Get-ResourceGroup -SubscriptionId $SubscriptionId -ResourceGroupName $ResourceGroupName
    If($ResourceGroup) {
        Write-Host "Removing Resource Group $ResourceGroupName"
        $ResourceGroup | Remove-AzResourceGroup -Verbose -Force
    }
}