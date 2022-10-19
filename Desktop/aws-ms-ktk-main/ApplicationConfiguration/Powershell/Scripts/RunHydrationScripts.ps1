param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$SQLServerName,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DatabaseName,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$UserName,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$Pwd,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$ScriptPath,
    [Parameter(Mandatory = $false, Position = 9)]
    [bool]$TruncateExisting = $true,
    [Parameter(Mandatory = $false, Position = 10)]
    [string]$DeployUATTesting = "false"
)

. $ModulesDirectory\SQL\SqlServerModule.ps1

if ($TruncateExisting -eq $true) {
    Write-Host "Truncating existing hydration" -ForegroundColor Cyan
    $SQL = "EXEC dbo.TruncateHydration @Confirm = 'CONFIRM'"
    Invoke-SQLCmdOnAzureSQLDatabase -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName -DatabaseName $DatabaseName -UserName $UserName -Pwd $Pwd -Command $SQL
}

$Scripts = $ScriptPath + "*" | Get-ChildItem -Include *.sql
ForEach ($Script in $Scripts.Name) {
    if (($Script -like "*uat*" -and $DeployUATTesting -eq "true") -or $Script -notlike "*uat*") {
        Write-Host "Running Hydration Script $Script" -ForegroundColor Cyan
        $Path = "$ScriptPath\$Script"
        Invoke-SQLScriptOnAzureSQLDatabase -ResourceGroupName $ResourceGroupName -SQLServerName $SQLServerName -DatabaseName $DatabaseName -UserName $UserName -Pwd $Pwd -ScriptPath $Path
    }
}

