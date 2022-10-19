#echo $PSVersionTable.PSVersion.ToString()
#Install-Module PowerShellGet -Repository PSGallery -Force
#Install-Module -Name Az.Storage -RequiredVersion 1.11.1-preview -AllowPrerelease -SkipPublisherCheck
#Connect-AzAccount

Function Get-StorageAccounts {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName
    )
    $StorageAccounts = Get-AzStorageAccount -ResourceGroupName $ResourceGroupName
    return $StorageAccounts
}

Function Get-StorageAccount {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName
    )
    $StorageAccounts = Get-StorageAccounts -ResourceGroupName $ResourceGroupName
    $StorageAccount = $StorageAccounts | Where-Object {$_.StorageAccountName -eq $StorageAccountName}
    if($StorageAccount) {
        return $StorageAccount
    } else {
        Write-Host "Storage Account does not exist."
    }
}

Function New-StorageAccount {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$SkuName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Location,
        [Parameter(Mandatory=$true, Position=4)]
        [string]$Kind,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$AccessTier,
        [Parameter(Mandatory=$true, Position=6)]
        [boolean]$EnableHttpsTrafficOnly,
        [Parameter(Mandatory=$true, Position=7)]
        [boolean]$EnableHierarchicalNamespace
    )
    $StorageAccount = Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    if(!$StorageAccount) {
        New-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -SkuName $SkuName -Location $Location -Kind $Kind -AccessTier $AccessTier -EnableHttpsTrafficOnly $EnableHttpsTrafficOnly -EnableHierarchicalNamespace $EnableHierarchicalNamespace
    } else {
        Write-Host "Storage Account already exists."
    }
}

Function Remove-StorageAccount {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName
    )
    $StorageAccount = Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    if($StorageAccount) {
        Remove-AzStorageAccount -ResourceGroupName $ResourceGroupName -Name $StorageAccountName -Force
    } else {
        Write-Host "Storage Account does not exist."
    }
}

Function Get-FileSystems {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    if($Context) {
        $FileSystems = Get-AzDataLakeGen2FileSystem -Context $Context
        return $FileSystems
    } else {
        Write-Host "Storage Account does not exist."
    }
}

Function Get-FileSystem {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName
    )
    $FileSystems = Get-FileSystems -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    $FileSystem = $FileSystems | Where-Object {$_.Name -eq $FileSystemName}
    if($FileSystem) {
        return $FileSystem
    } else {
        Write-Host "File System does not exist."
    }
}

Function New-FileSystem {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $FileSystem = Get-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName -FileSystemName $FileSystemName
    if(!$FileSystem) {
        New-AzDatalakeGen2FileSystem -Context $Context -Name $FileSystemName
    } else {
        Write-Host "File System already exists."
    }
}

Function Remove-FileSystem {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $FileSystem = Get-FileSystem -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName -FileSystemName $FileSystemName
    if($FileSystem) {
        Remove-AzDatalakeGen2FileSystem -Context $Context -Name $FileSystemName
    } else {
        Write-Host "File System does not exist."
    }
}
Function Get-StorageAccountContext {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName
    )

    Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    return $Context
}

Function Get-StorageAccountKeys {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName
    )
    $StorageAccount = Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName
    if($StorageAccount) {
        $StorageAccountKeys = Get-AzStorageAccountKey -ResourceGroupName $ResourceGroupName -Name $StorageAccountName
        return $StorageAccountKeys
    } else {
        Write-Host "Storage Account does not exist."
    }
}

Function New-Gen2BaseDirectory {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $BaseDirectoryItems = Get-AzDataLakeGen2ChildItem -Context $Context -FileSystem $FileSystemName
    $Directory = $BaseDirectoryItems | Where-Object {$_.Path -eq "$Path/"}
    if(!$Directory) {
        New-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path -Directory
    } else {
        Write-Host "Directory already exists."
    }
}

Function Remove-Gen2BaseDirectory {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $BaseDirectoryItems = Get-AzDataLakeGen2ChildItem -Context $Context -FileSystem $FileSystemName
    $Directory = $BaseDirectoryItems | Where-Object {$_.Path -eq "$Path/"}
    if($Directory) {
        Remove-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path -Force
    } else {
        Write-Host "Directory does not exist."
    }
}

Function Get-ACLPermissionsForItem {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $Item = Get-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path
    return $Item.ACL
}

Function Set-ACLDefaultScopePermissionsForItem {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path,
        [Parameter(Mandatory=$true, Position=4)]
        [string]$Permission,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$Id,
        [Parameter(Mandatory=$true, Position=6)]
        [string]$AccessControlType #user, group
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $ACL = (Get-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path).ACL
    [Collections.Generic.List[System.Object]]$ACLNew = $ACL
    foreach ($A in $ACLNew) {
        if ($A.AccessControlType -eq $AccessControlType -and $A.DefaultScope -eq $true -and $A.EntityId -eq $Id) {
            $ACLNew.Remove($A);
            Break;
        }
    }
    $ACLNew = New-AzDataLakeGen2ItemAclObject -AccessControlType user -EntityId $Id -Permission $Permission -DefaultScope -InputObject $ACLNew
    Update-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path -Acl $ACLNew
}

Function Set-ACLRecursivePermissions {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path,
        [Parameter(Mandatory=$true, Position=4)]
        [string]$Permission,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$Id,
        [Parameter(Mandatory=$true, Position=6)]
        [string]$AccessControlType #user, group
    )
    $Context = (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context
    $ACL = (Get-AzDataLakeGen2Item -Context $Context -FileSystem $FileSystemName -Path $Path).ACL
    $ACL = New-AzDataLakeGen2ItemAclObject -AccessControlType $AccessControlType -EntityId $Id -Permission $Permission -InputObject $ACL
    Get-AzDataLakeGen2ChildItem -Context $Context -FileSystem $FileSystemName -Path $Path -Recurse | Update-AzDataLakeGen2Item -Acl $ACL
}

Function Get-DirectoryContents {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Path,
        [Parameter(Mandatory=$false, Position=4)]
        [bool]$Recurse = $false
    )
    if ($Recurse -eq $true) {
        Get-AzDataLakeGen2ChildItem -Context (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context -FileSystem $FileSystemName -Path $Path -Recurse
    } else {
        Get-AzDataLakeGen2ChildItem -Context (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context -FileSystem $FileSystemName -Path $Path
    }
}

Function Copy-LocalFilesToDirectory {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$StorageAccountName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$FileSystemName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$LocalDirectory,
        [Parameter(Mandatory=$false, Position=4)]
        [bool]$Recurse = $false,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$DestinationDirectory
    )

    if ($Recurse -eq $true) {
        $Files = (Get-ChildItem $LocalDirectory -Recurse | Where-Object {$_.Mode -eq "-a---"}).FullName
    } else {
        $Files = (Get-ChildItem $LocalDirectory | Where-Object {$_.Mode -eq "-a---"}).FullName
    }

    ForEach ($File in $Files) {
        $DestinationFileName = $File.Replace($LocalDirectory, $DestinationDirectory).Replace("\","/")
        New-AzDataLakeGen2Item -Context (Get-StorageAccount -ResourceGroupName $ResourceGroupName -StorageAccountName $StorageAccountName).Context -FileSystem $FileSystemName -Path $DestinationFileName -Source $File -Force
    }
}