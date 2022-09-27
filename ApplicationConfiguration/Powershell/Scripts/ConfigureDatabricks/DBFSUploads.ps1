param
(
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$URIBase,
    [Parameter(Mandatory = $true, Position = 2)]
    [securestring]$Token,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$InitScriptPath,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$SchemasDirectory,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$DQRulesDirectory,
    [Parameter(Mandatory = $true, Position = 6)]
    [string]$DeployDatabricksClusters,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$DeployDatabricksNotebooks,
    [Parameter(Mandatory = $true, Position = 7)]
    [string]$RunDataQualityNotebooks,
    [Parameter(Mandatory = $true, Position = 8)]
    [string]$DeployJobs
)

#region begin DBFS Uploads
$DestinationPath = "/databricks/pyodbc.sh"
if ($DeployDatabricksClusters -eq "true") {
    Write-Host "Uploading Default Cluster Init Script" -ForegroundColor Cyan
    Import-LocalFileToDBFS -BaseURI $URIBase -Token $Token -Path $DestinationPath -LocalFile $InitScriptPath -Overwrite $true
}

if ($RunDataQualityNotebooks -eq "true") {
    if ($DQRulesDirectory -ne "") {
        Write-Host "Uploading Data Quality Rules" -ForegroundColor Cyan
        $LocalPath = "$DQRulesDirectory/goldprotected.vActiveDataQualityRule.json"
        $DQRulesDestinationPath = "/FileStore/import-stage/goldprotected.vActiveDataQualityRule.json"
        Import-LocalFileToDBFS -BaseURI $URIBase -Token $Token -Path $DQRulesDestinationPath -LocalFile $LocalPath -Overwrite $true
    }
}
if ($DeployDatabricksClusters -eq "true" -or $DeployJobs -eq "true" -or $DeployDatabricksNotebooks -eq "true") {
    $WheelFolderPath = $ModulesDirectory.Replace("/ApplicationConfiguration/Powershell/Modules", "/python-wheel/*")
    $LocalLibPath = @($WheelFolderPath | Get-ChildItem -Include *.whl)[0]
    $FileName = Split-Path $LocalLibPath -leaf
    $DBFSPath = "dbfs:/FileStore/jars/"+$FileName
    Write-Host "Uploading Databricks Framework Python Library" -ForegroundColor Cyan
    Import-LocalFileToDBFS -BaseURI $URIBase -Token $Token -Path $DBFSPath -LocalFile $LocalLibPath -Overwrite $true
}
#placeholder for SchemasDirectory (todo: add recursive dbfs upload)
#endregion
