# New-SqlDatabase
# Remove-SqlDatabase

Function Get-Databases {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName
    )
    $Databases = Get-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName
    return $Databases
}

Function Get-Database {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$DatabaseName
    )
    $Databases = Get-Databases -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName
    $Database = $Databases | Where-Object {$_.DatabaseName -eq $DatabaseName}
    if($Database) {
        return $Database
    } else {
        Write-Host "Database does not exist."
    }
}

Function New-Database {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$Edition
    )
    $Database = Get-Database -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName -DatabaseName $DatabaseName
    if(!$Database) {
        New-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName -DatabaseName $DatabaseName -Edition $Edition
    } else {
        Write-Host "Database already exists."
    }
}

Function Remove-Database {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$DatabaseName
    )
    $Database = Get-Database -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName -DatabaseName $DatabaseName
    if($Database) {
        Remove-AzSqlDatabase -ResourceGroupName $ResourceGroupName -ServerName $SQLServerName -DatabaseName $DatabaseName -Force
    } else {
        Write-Host "Database does not exist."
    }
}


