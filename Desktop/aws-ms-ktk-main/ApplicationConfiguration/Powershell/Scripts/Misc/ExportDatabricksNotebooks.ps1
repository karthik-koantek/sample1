param
(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$SubscriptionId,
    [Parameter(Mandatory = $true, Position = 1)]
    [string]$ResourceGroupName,
    [Parameter(Mandatory = $true, Position = 2)]
    [string]$BearerToken,
    [Parameter(Mandatory = $true, Position = 3)]
    [string]$ModulesDirectory,
    [Parameter(Mandatory = $true, Position = 4)]
    [string]$NotebookDirectory,
    [Parameter(Mandatory = $true, Position = 5)]
    [string]$OutputPath,
    [Parameter(Mandatory=$false, Position = 6)]
	[string]$ExportFormat = "SOURCE",
    [Parameter(Mandatory=$false, Position = 7)]
    [bool]$DirectDownload = $true,
    [Parameter(Mandatory=$false, Position = 8)]
    [bool]$Recursive = $true
)

Set-AzContext -SubscriptionId $SubscriptionId
Install-Module -Name Az.Databricks -Force

$DatabricksWorkspace = Get-AzDatabricksWorkspace -ResourceGroupName $ResourceGroupName
[string]$DatabricksWorkspaceUrl = $DatabricksWorkspace.Url
[string]$Machine = "https://$DatabricksWorkspaceUrl"
[string]$APIVersion = "2.0"
[string]$URIBase = "$Machine/api/$APIVersion"
$Token = ConvertTo-SecureString -String $BearerToken -AsPlainText -Force

. $ModulesDirectory\Databricks\DatabricksWorkspace.ps1

$NotebookDirectory = "/Framework"
$OutputPath = "C:\src\ktkaz\Azure Databricks\AzureDatabricks\FrameworkNotebooks"
$ExportFormat = "SOURCE"

Export-NotebookDirectoryRecursive -BaseURI $URIBase -Token $Token -NotebookDirectoryBase $NotebookDirectory -NotebookDirectory $NotebookDirectory -DirectDownload $true -OutputPathBase $OutputPath -OutputPath $OutputPath -ExportFormat $ExportFormat
