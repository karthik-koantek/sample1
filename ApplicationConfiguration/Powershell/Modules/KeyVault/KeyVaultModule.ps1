#Install-Module -Name Az.KeyVault
Function Get-KeyVault {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KeyVaultName
    )
    $KeyVault = Get-AzKeyVault -ResourceGroupName $ResourceGroupName -VaultName $KeyVaultName
    return $KeyVault
}

Function New-KeyVault {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KeyVaultName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$Location,
        [Parameter(Mandatory=$true, Position = 3)]
        [string]$Sku
    )

    $KeyVault = Get-KeyVault -ResourceGroupName $ResourceGroupName -KeyVaultName $KeyVaultName
    if(!$KeyVault) {
        New-AzKeyVault -ResourceGroupName $ResourceGroupName -VaultName $KeyVaultName -Location $Location -EnabledForDeployment -EnabledForTemplateDeployment -EnabledForDiskEncryption -Sku $Sku
    } else {
        Write-Host "Key Vault already exists."
    }
}

Function Remove-KeyVault {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$KeyVaultName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$Location
    )

    $KeyVault = Get-KeyVault -ResourceGroupName $ResourceGroupName -KeyVaultName $KeyVaultName
    if($KeyVault) {
        Remove-AzKeyVault -ResourceGroupName $ResourceGroupName -VaultName $KeyVaultName -Location $Location -Force
    } else {
        Write-Host "Key Vault does not exist."
    }
}

Function Set-KeyVaultSecret {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$KeyVaultName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$SecretName,
        [Parameter(Mandatory=$true, Position = 2)]
        [string]$SecretValue
    )
    $SecureSecretValue = ConvertTo-SecureString -String $SecretValue -AsPlainText -Force
    Set-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretName -SecretValue $SecureSecretValue
}

Function Remove-KeyVaultSecret {
    param
    (
        [Parameter(Mandatory=$true, Position = 0)]
        [string]$KeyVaultName,
        [Parameter(Mandatory=$true, Position = 1)]
        [string]$SecretName
    )
    $found = Get-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretName
    if($found) {
        Remove-AzKeyVaultSecret -VaultName $KeyVaultName -Name $SecretName -PassThru -Force
    } else {
        Write-Host "Key Vault Secret does not exist."
    }
}





