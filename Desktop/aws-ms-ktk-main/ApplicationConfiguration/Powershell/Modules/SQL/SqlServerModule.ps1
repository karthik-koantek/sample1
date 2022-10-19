#Install-Module -Name SqlServer

function Invoke-SQLScriptOnAzureSQLDatabase {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$UserName,
        [Parameter(Mandatory=$true, Position=4)]
        [string]$Pwd,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$ScriptPath
    )
    Invoke-Sqlcmd -ServerInstance $SQLServerName -Database $DatabaseName -Username $UserName -Password $Pwd -InputFile $ScriptPath
}

function Invoke-SQLCmdOnAzureSQLDatabase {
    param
    (
        [Parameter(Mandatory=$true, Position=0)]
        [string]$ResourceGroupName,
        [Parameter(Mandatory=$true, Position=1)]
        [string]$SQLServerName,
        [Parameter(Mandatory=$true, Position=2)]
        [string]$DatabaseName,
        [Parameter(Mandatory=$true, Position=3)]
        [string]$UserName,
        [Parameter(Mandatory=$true, Position=4)]
        [string]$Pwd,
        [Parameter(Mandatory=$true, Position=5)]
        [string]$Command
    )
    Invoke-Sqlcmd -ServerInstance $SQLServerName -Database $DatabaseName -Username $UserName -Password $Pwd -Query $Command
}